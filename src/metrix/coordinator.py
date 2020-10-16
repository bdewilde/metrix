from typing import Dict, List, Optional, Sequence, Union

import streamz

from metrix import MSink, MStream


Number = Union[int, float]


class MCoordinator:
    """
    Class that coordinates the flow of metric elements through one or multiple streams
    into one or multiple sinks, with an optional rate limit imposed before the end.

    Here are a few simple examples to illustrate key features:

    .. code-block:: pycon

       >>> import statistics, time
       >>> from metrix import MCoordinator, MStream, MSinkLogger, MSinkPrinter
       >>> # one stream, one agg, one sink
       >>> mc = MCoordinator(
       ...     mstreams=[MStream("n", agg=sum, batch_size=2)],
       ...     msinks=[MSinkPrinter()],
       ... )
       >>> mc.send("n", 1)
       >>> mc.send("n", 2)
       MElement(name=n.sum, value=3, tags=None)
       >>> # one stream, two aggs, two sinks
       >>> mc = MCoordinator(
       ...     mstreams=[MStream("n", agg=[max, statistics.mean], batch_size=3)],
       ...     msinks=[MSinkPrinter(), MSinkLogger()],
       ... )
       >>> mc.send("n", 1)
       >>> mc.send("n", 2)
       >>> mc.send("n", 3)
       MElement(name=n.max, value=3, tags=None)
       MElement(name=n.mean, value=2, tags=None)
       INFO:metrix.sinks:MElement(name=n.max, value=3, tags=None)
       INFO:metrix.sinks:MElement(name=n.mean, value=2, tags=None)
       >>> # two streams, default and element tags, and a timer
       >>> mc = MCoordinator(
       ...     mstreams=[
       ...         MStream("n", agg=sum, batch_size=3, default_tags={"foo": "bar"}),
       ...         MStream("time", agg={"avg": statistics.mean}, window_size=1),
       ...     ],
       ...     msinks=[MSinkPrinter()],
       ... )
       >>> mc.send("n", 1)
       >>> mc.send("n", 1)
       >>> mc.send("n", 1, tags={"foo": "BAR!"})
       MElement(name=n.sum, value=2, tags={'foo': 'bar'})
       MElement(name=n.sum, value=1, tags={'foo': 'BAR!'})
       >>> with mc.timer("time", scale=1):
       ...     time.sleep(0.5)
       >>> with mc.timer("time", scale=1):
       ...     time.sleep(0.75)
       >>> with mc.timer("time", scale=1):
       ...     time.sleep(0.5)
       >>> with mc.timer("time", scale=1):
       ...     time.sleep(0.25)
       MElement(name=time.avg, value=0.5028860028833151, tags=None)
       MElement(name=time.avg, value=0.7517436337657273, tags=None)
       MElement(name=time.avg, value=0.377787658944726, tags=None)

    In typical production usage, you'll be tracking a few metrics and periodically
    logging and/or sending aggregated values to TSDB. Here's how that might look:

    .. code-block:: pycon

       >>> from metrics import MSinkTSDB
       >>> mc = MCoordinator(
       ...     mstreams=[
       ...         MStream("n_msgs", agg=sum, window_size=3),
       ...         MStream("msg_len", agg=[statistics.mean, statistics.stdev], window_size=5)
       ...     ],
       ...     msinks=[MSinkLogger(), MSinkTSDB(<TSDB_CLIENT>)],
       ...     rate_limit=[0, 1.0],
       ... )
       >>> msgs = list(range(10))  # fake data ;)
       >>> for msg in msgs:
       ...     mc.send("n_msgs", 1)
       ...     mc.send("msg_len", msg)
       INFO:metrix.sinks:MElement(name=n_msgs.sum, value=10, tags=None)
       INFO:metrix.sinks:MElement(name=msg_len.mean, value=4.5, tags=None)
       INFO:metrix.sinks:MElement(name=msg_len.stdev, value=3.0276503540974917, tags=None)

    Args:
        mstreams: One or more :class:`MStream <metrix.stream.MStream>` s through which
            metric elements are sent. Typically provided on init, but may also be passed
            individually via :meth:`MCoordinator.add_mstream()`.
        msinks: One or more :class:`MSink <metrix.sinks.MSink>` s to which metric elements
            are sent. Typically provided on init, but may also be passed individually
            via :meth:`MCoordinator.add_msink()`. In a development context, the simple
            ``MSinkPrinter`` will give visibility into the outputs of metric
            streams, but in a production, you'll want to specify more persistent
            metric sinks like ``MSinkLogger`` and ``MSinkTSDB``.
        rate_limit: Optional rate limit that prevents two metric elements from
            streaming into a sink in an interval shorter than ``rate_limit`` seconds.
            If a single number, this is applied to all ``msinks``; if a sequence of
            numbers with the same length as ``msinks``, limits will be applied element-
            wise to the corresponding metric sinks.

            For example: ``rate_limit=1.5`` causes elements to be sent on to each sink
            in ``msinks`` at least 1.5 seconds apart. If ``rate_limit=[1.5, 0.5]`` (and
            two sinks are specified), then the first sink will have a 1.5-second rate
            limit while the second will have a 0.5-second rate limit applied.

    .. warning:: If ``MSinkTSDB`` is added as a sink, be sure to have ``rate_limit``
       set to at least 1.0 seconds to prevent data loss, since OpenTSDB doesn't support
       sub-second data. (Yes, I know -- it's bonkers.)

       Given this constraint, you must also be mindful of the total number of unique
       metric (name, agg, tags) pairs passing through ``mstreams`` per second to ensure
       that the output sinks can keep up with the rate of input metrics. For example,
       if 2 streams use a single aggregator with default ``window_size=10`` and
       ``rate_limit=1.0``, then you should limit yourself to no more than 5 distinct
       tag sets per metric. If you have more tags or aggs, increase your window size
       accordingly! Here's a useful formula::

           sum((num_aggs * num_unique_tag_sets / window_size) for stream in mstreams) = num_total_metrics_per_sec

       If ``num_total_metrics_per_sec > rate_limit``, you have a problem.

    Attributes:
        stream: Base metric coordinator stream, to which metric streams connect and
            from which metric sinks extend.
        metric_mstreams: Mapping of metric name to metric stream, each of which is
            upstream from and connected to :attr:`MCoordinator.stream`.
        msinks: Sequence of metric sinks, each of which is downstream from
            and connected to :attr:`MCoordinator.stream`.
    """

    stream: streamz.Stream
    metric_mstreams: Dict[str, MStream]
    msinks: List[MSink]

    def __init__(
        self,
        *,
        mstreams: Optional[Sequence[MStream]] = None,
        msinks: Optional[Sequence[MSink]] = None,
        rate_limit: Optional[Union[Number, Sequence[Number]]] = None,
    ):
        self.metric_mstreams = {}
        self.msinks = []
        self._build_stream(mstreams, msinks, rate_limit)

    def __str__(self):
        return (
            "MCoordinator(\n"
            f"    mstreams={[str(mstream) for mstream in self.metric_mstreams.values()]},\n"
            f"    msinks={self.msinks},\n"
            ")"
        )

    def _build_stream(
        self,
        mstreams: Optional[Sequence[MStream]],
        msinks: Optional[Sequence[MSink]],
        rate_limit: Optional[Union[Number, Sequence[Number]]],
    ):
        """
        Build a stream based on user-specified attributes, including an entry point,
        one or more upstream metric streams, and one or more downstream sink streams,
        which themselves consist of a buffer and sink with optional rate limiter.
        """
        # entry point to the sink streams
        self.stream = streamz.Stream(stream_name="MC")
        # add metric streams
        if mstreams:
            for mstream in mstreams:
                self.add_mstream(mstream)
        # add sink streams
        if msinks:
            # validate and transform rate_limit arg into a sequence of per-sink values
            if rate_limit is None or isinstance(rate_limit, (int, float)):
                rate_limits = [rate_limit for _ in range(len(msinks))]
            elif isinstance(rate_limit, Sequence) and not isinstance(rate_limit, str):
                if len(rate_limit) != len(msinks):
                    raise ValueError(
                        f"rate_limit={rate_limit} is incompatible with msinks={msinks}; "
                        "they must have the same number of elements"
                    )
                else:
                    rate_limits = rate_limit
            else:
                raise TypeError(
                    f"rate_limit={rate_limit} is invalid; "
                    "must be None, a number, or a sequence of numbers"
                )
            for msink, rl in zip(msinks, rate_limits):
                self.add_msink(msink, rate_limit=rl)

    def add_mstream(self, mstream: MStream) -> None:
        """
        Add a metric stream to this coordinator by connecting it to all sink streams
        and making it accessible by name via :attr:`MCoordinator.metric_mstreams`.
        """
        mstream.stream.connect(self.stream)
        self.metric_mstreams[mstream.name] = mstream

    def add_msink(self, msink: MSink, rate_limit: Optional[Number] = None) -> None:
        """
        Add a metric sink to this coordinator by branching off :attr:`MCoordinator.stream`
        with a buffered, optionally rate-limited stream that ends in ``msink``.
        """
        # add a small buffer before sink, in case we experience a transient pile-up
        buffer_size = min(3 * len(self.metric_mstreams), 3)
        if rate_limit:
            self.stream.rate_limit(rate_limit).buffer(buffer_size).sink(msink)
        else:
            self.stream.buffer(buffer_size).sink(msink)
        self.msinks.append(msink)

    def send(self, name: str, value: Number, *, tags: Optional[Dict] = None) -> None:
        """
        Send a metric value to a particular metric stream; optionally, pass tags
        to add new and overwrite existing default tags associated with the stream.

        Args:
            name: Metric name.
            value: Numeric metric value.
            tags: Optional tags to associate with this specific metric ``value``.

        .. seealso:: :meth:`MStream.send() <metrix.stream.MStream.send>`
        """
        self.metric_mstreams[name].send(value=value, tags=tags)

    def timer(self, name: str, scale: int = 1, *, tags: Optional[Dict] = None):
        """
        Get a context manager for a particular stream that measures the elapsed time spent
        running statements enclosed by the ``with`` statement, and sends that time
        to the stream.

        Args:
            scale: Multiplier applied to the elapsed time value, in seconds by default.
                For example, to report time in milliseconds, use ``scale=1000``.
            tags: Optional tags to associate with this specific timer value.

        See Also:
            :meth:`MStream.timer() <metrix.stream.MStream.timer>`
        """
        return self.metric_mstreams[name].timer(scale=scale, tags=tags)
