from contextlib import contextmanager
from functools import partial
from operator import attrgetter
from timeit import default_timer
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import streamz
from toolz import itertoolz

import metrix.element
from metrix import MElement


Number = Union[int, float]
AggFunc = Callable[[Iterable[Number]], Number]
MetricAggFunc = Callable[[Dict[Optional[str], Iterable[MElement]]], Tuple[MElement]]


class MStream:
    """
    A stream of :class:`MElement` s that groups elements into batches of fixed
    time or number, further groups batches by distinct assigned tags, then aggregates
    each group's values by one or multiple functions.

    To do any useful work, metric streams must be connected to an ``MSink``, which
    operates on elements in a visible / persistent way. In typical usage, you'll want
    to connect multiple streams to multiple sinks using a ``MCoordinator``.
    Refer to that class for a more realistic usage example than what follows here:

    .. code-block:: pycon

       >>> from metrix import MElement, MStream
       >>> eles = [{"value": 1}, {"value": 2}, {"value": 1, "tags": {"foo": "bar"}}]
       >>> mstream = MStream("m", agg=sum, default_tags={"foo": "BAR!"}, window_size=1)
       >>> # HACK! we'll add a sink directly so we can see what happens
       >>> mstream.stream.sink(print)
       >>> for ele in eles:
       ...     mstream.send(**ele)
       MElement(name=m.sum, value=3, tags={'foo': 'BAR!'})
       MElement(name=m.sum, value=1, tags={'foo': 'bar'})

    Args:
        name: Name of the metric whose elements are sent into this stream.
        agg: One or multiple aggregation functions to be applied to groups of metric
            elements' values in order to produce new, aggregated metric elements.
            This may be specified as a single callable or a sequence of callables,
            in which case the corresponding components of the :attr:`MStream.stream`
            are named after the functions themselves; this may also be specified
            as a mapping of component name to callable, in which case the user-specified
            names are used instead.
        default_tags: Optional set of tags to apply to all metric elements by default.
            Tags specified on individual elements override and append to this def
        window_size: Size of tumbling window in *seconds* with which to group elements.
            For example: If ``window_size=10``, all elements sent into the stream
            within a given 10-second window will be grouped together before
            their values are aggregated, as specified by ``agg``.
        batch_size: Size of batches in *number of elements* with which to group elements.
            For example: If ``batch_size=10``, every 10 successive elements sent into
            the stream will be grouped together before their values are aggregated,
            as specified by ``agg``. Note that setting ``batch_size=1`` will effectively
            skip grouping, in which case aggregating values doesn't make sense, either.

    Note:
        You *must* set either ``window_size`` or ``batch_size`` when initializing
        a metric stream. No default is set because it depends entirely on context:
        the rate with which metric elements are sent into the stream, the desired
        resolution on aggregated metrics, and any rate limit requirements on connected
        metric sinks. This is the only stream attribute that demands deliberate thought.
        Choose wisely! :)

    Attributes:
        name
        agg
        default_tags
        window_size
        batch_size
        source: Entry point to the metric stream.
        stream: Data processing stream to which metric elements are sent.
    """

    source: streamz.Source
    stream: streamz.Stream

    def __init__(
        self,
        name: str,
        *,
        agg: Union[AggFunc, Sequence[AggFunc], Mapping[str, AggFunc]],
        default_tags: Optional[Dict] = None,
        window_size: Optional[int] = None,
        batch_size: Optional[int] = None,
    ):
        self._validate_sizes(window_size, batch_size)
        self.name = name
        self.agg = agg
        self.default_tags = default_tags
        self.window_size = window_size
        self.batch_size = batch_size
        self._build_stream()

    def __str__(self):
        return f"MStream(name={self.name}, agg={self.agg})"

    def _validate_sizes(self, window_size: Optional[int], batch_size: Optional[int]):
        if not bool(window_size) ^ bool(batch_size):
            raise ValueError(
                "either window_size or batch_size must be specified in order to group "
                "metric elements prior to value aggregation -- but not neither, not both"
            )
        if (
            (isinstance(window_size, int) and window_size < 0) or
            (isinstance(batch_size, int) and batch_size < 0)
        ):
            raise ValueError(
                "if specified, window_size and batch_size must be non-negative integers"
            )

    def _build_stream(self) -> None:
        """
        Build a stream based on user-specified attributes, including a source entry point,
        a fixed-time or -size grouper, an additional grouper by distinct tag set,
        and one or multiple value aggregators. Does not include any sinks!
        """
        metric_aggs = self._process_agg(self.agg)
        # build stream source and base stream that groups metric elements together
        self.source = streamz.Source(stream_name=self.name)
        if self.window_size is not None:
            base_stream = (
                # entry point for the stream
                self.source
                # batch elements in tumbling windows of `window_size` seconds
                .timed_window(self.window_size, stream_name="group_by_time")
            )
        elif self.batch_size is not None:
            base_stream = (
                # entry point for the stream
                self.source
                # batch elements into equal-size batches of `batch_size` elements apiece
                .partition(self.batch_size, stream_name="group_by_num")
            )
        else:
            raise ValueError("this shouldn't ever happen -- developer error?")

        grped_stream = (
            base_stream
            # filter out groups without any elements
            .filter(group_has_elements)
            # further group group elements by their `key`, which effectively creates
            # distinct metrics for each combination of (name, tags)
            .map(partial(itertoolz.groupby, attrgetter("key")), stream_name="group_by_key")
        )
        # if only one agg, we can build a simple stream
        if len(metric_aggs) == 1:
            metric_aggname, metric_aggfunc = metric_aggs[0]
            self.stream = (
                # start from a mapping of key to window elements
                grped_stream
                # aggregate the values of each key group's elements and
                # output a sequence of aggregated elements (one per group)
                .map(metric_aggfunc, stream_name=metric_aggname)
                # flatten sequence of agg elements, so each is its own item in stream
                .flatten(stream_name="flatten_groups")
            )
        # otherwise, we'll need to branch and double-flatten
        else:
            # for each agggretor, create a new branch off the grouped stream
            # that aggregates each group's elements, as in the simple stream case
            self._agg_streams = tuple(
                grped_stream.map(metric_aggfunc, stream_name=metric_aggname)
                for metric_aggname, metric_aggfunc in metric_aggs
            )
            # zip each agg stream's outputs together, to keep aggregated results in sync
            self.stream = (
                streamz.zip(*self._agg_streams, stream_name="zip_aggs")
                # flatten out the zipped aggs
                .flatten(stream_name="flatten_aggs")
                # then flatten out the agg elements for each key group
                .flatten(stream_name="flatten_groups")
            )

    def _process_agg(
        self, agg: Union[AggFunc, Sequence[AggFunc], Mapping[str, AggFunc]]
    ) -> List[Tuple[str, MetricAggFunc]]:
        """
        Process user-provided ``agg`` into a standard form:
        a sequence of (name, func) pairs that aggregate groups of metric element values.
        """
        if isinstance(agg, Mapping):
            metric_aggs = [self._make_metric_agg(key, val) for key, val in agg.items()]
        elif isinstance(agg, Sequence) and not isinstance(agg, str):
            metric_aggs = [self._make_metric_agg(item.__name__, item) for item in agg]
        elif isinstance(agg, Callable):
            metric_aggs = [self._make_metric_agg(agg.__name__, agg)]
        else:
            raise TypeError(
                f"agg={agg} is invalid; "
                "must be of type Union[AggFunc, Sequence[AggFunc], Dict[str, AggFunc]]"
            )
        return metric_aggs

    def _make_metric_agg(self, aggname: str, aggfunc: AggFunc) -> Tuple[str, MetricAggFunc]:
        """
        From a user-provided aggregator, make a "metric aggregator":
        a (name, func) pair that aggregates groups of metric element values.
        """
        def metric_aggfunc(
            name: str,
            aggfunc: AggFunc,
            grped_mes: Dict[Optional[str], Iterable[MElement]],
        ) -> Tuple[MElement]:
            return tuple(
                MElement(
                    name=name,
                    value=aggfunc(me.value for me in grp_mes),
                    tags=metrix.element.tags_from_key(grp_key),
                )
                for grp_key, grp_mes in grped_mes.items()
            )

        metric_aggname = f"{self.name}.{aggname}"
        return (metric_aggname, partial(metric_aggfunc, metric_aggname, aggfunc))

    def send(self, value: Number, *, tags: Optional[Dict] = None) -> None:
        """
        Send a given metric value to the stream; optionally, pass metric-specific tags
        to add new and overwrite existing default tags associated with the stream.

        Args:
            value: Numeric metric value.
            tags: Optional tags to associate with this specific metric ``value``.
        """
        if self.default_tags and tags:
            tags = {**self.default_tags, **tags}
        else:
            tags = tags or self.default_tags
        me = MElement(self.name, value, tags=tags)
        self.source.emit(me)

    @contextmanager
    def timer(self, scale: int = 1, *, tags: Optional[Dict] = None):
        """
        Context manager that measures the elapsed time spent running statements
        enclosed by the ``with`` statement, and sends that time to the stream.

        Args:
            scale: Multiplier applied to the elapsed time value, in seconds by default.
                For example, to report time in milliseconds, use ``scale=1000``.
            tags: Optional tags to associate with this specific timer value.

        See Also:
            :meth:`MStream.send()`
        """
        start = default_timer()
        try:
            yield
        finally:
            end = default_timer()
            self.send((end - start) * scale, tags=tags)


def group_has_elements(group: Sequence[MElement]) -> bool:
    """
    Return True if ``group`` contains any metric elements, and False otherwise;
    used to filter out empty group from a metric stream.
    """
    return bool(group)
