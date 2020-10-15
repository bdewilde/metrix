import statistics
import time

import pytest
import streamz

from metrix import MElement, MStream, MSinkPrinter
from metrix import MCoordinator as MC


@pytest.fixture(scope="module")
def test_elements():
    return [
        {"name": "m1", "value": 1, "tags": None},
        {"name": "m2", "value": 2, "tags": {"foo": "bar"}},
        {"name": "m1", "value": 3, "tags": None},
        {"name": "m2", "value": 1, "tags": {"bat": "baz"}},
        {"name": "m2", "value": 2, "tags": {"foo": "bar"}},
        {"name": "m1", "value": 3, "tags": None},
        {"name": "m2", "value": 1, "tags": {"foo": "bar"}},
        {"name": "m1", "value": 1, "tags": {"foo": "bar"}},
        {"name": "m1", "value": 3, "tags": {"bat": "baz"}},
        {"name": "m2", "value": 2, "tags": None},
    ]


class MSinkToList:
    """Ad-hoc metric sink -- useful for testing, but not production."""

    def __init__(self):
        self.data = []

    def __call__(self, me: MElement):
        self.data.append(me)


@pytest.mark.parametrize(
    "mstreams,msinks,rate_limit",
    [
        (
            [MStream("m1", agg=sum, batch_size=1)],
            [MSinkPrinter()],
            1.0,
        ),
        (
            [MStream("m1", agg=sum, batch_size=1)],
            [MSinkPrinter()],
            None,
        ),
        (
            [
                MStream("m1", agg=sum, batch_size=1),
                MStream("m2", agg=[min, max], batch_size=1)
            ],
            [MSinkPrinter(), MSinkPrinter()],
            [1.0, 0.5],
        ),
        (None, None, None),
    ]
)
def test_metric_coordinator_init(mstreams, msinks, rate_limit):
    mc = MC(mstreams=mstreams, msinks=msinks, rate_limit=rate_limit)
    assert all(hasattr(mc, attr) for attr in ["stream", "metric_mstreams", "msinks"])
    if mstreams:
        assert (
            len(mc.metric_mstreams) == len(mc.stream.upstreams) == len(mstreams) and
            sorted(mc.metric_mstreams.keys()) == sorted(mstream.name for mstream in mstreams)
        )
    if msinks:
        assert len(mc.msinks) == len(mc.stream.downstreams) == len(msinks)
        if rate_limit:
            assert all(isinstance(ds, streamz.core.rate_limit) for ds in mc.stream.downstreams)
            if isinstance(rate_limit, list):
                assert all(ds.interval == rl for ds, rl in zip(mc.stream.downstreams, rate_limit))
            else:
                assert all(ds.interval == rate_limit for ds in mc.stream.downstreams)
        else:
            assert all(isinstance(ds, streamz.core.buffer) for ds in mc.stream.downstreams)


@pytest.mark.parametrize(
    "mstreams",
    [
        [MStream("m1", agg=sum, batch_size=1)],
        [
            MStream("m1", agg=sum, batch_size=1),
            MStream("m2", agg=[min, max], batch_size=1)
        ],
    ]
)
def test_metric_coordinator_add_mstream(mstreams):
    mc = MC()
    for mstream in mstreams:
        mc.add_mstream(mstream)
    assert (
        len(mc.metric_mstreams) == len(mc.stream.upstreams) == len(mstreams) and
        sorted(mc.metric_mstreams.keys()) == sorted(mstream.name for mstream in mstreams)
    )


@pytest.mark.parametrize(
    "msinks,rate_limits",
    [
        ([MSinkPrinter()], [1.0]),
        ([MSinkPrinter(), MSinkPrinter()], [1.0, 0.5]),
        ([MSinkPrinter()], [None]),
    ]
)
def test_metric_coordinator_add_msink(msinks,rate_limits):
    mc = MC()
    for sink, rate_limit in zip(msinks, rate_limits):
        mc.add_msink(sink, rate_limit)
    assert len(mc.msinks) == len(mc.stream.downstreams) == len(msinks)


@pytest.mark.parametrize(
    "init_kwargs,exp_results",
    [
        (
            {
                "mstreams": [
                    MStream("m1", agg=sum, batch_size=5),
                    MStream("m2", agg=statistics.mean, batch_size=5)
                ],
                "msinks": [MSinkToList(), MSinkToList()]
            },
            [
                MElement(name="m1.sum", value=7, tags=None),
                MElement(name="m1.sum", value=1, tags={"foo": "bar"}),
                MElement(name="m1.sum", value=3, tags={"bat": "baz"}),
                MElement(name="m2.mean", value=1.6666666666666667, tags={"foo": "bar"}),
                MElement(name="m2.mean", value=1, tags={"bat": "baz"}),
                MElement(name="m2.mean", value=2, tags=None),
            ],
        ),
        (
            {
                "mstreams": [
                    MStream("m1", agg=sum, batch_size=1),
                    MStream("m2", agg=statistics.mean, batch_size=1)
                ],
                "msinks": [MSinkToList(), MSinkToList()]
            },
            [
                MElement(name="m1.sum", value=1, tags=None),
                MElement(name="m2.mean", value=2, tags={"foo": "bar"}),
                MElement(name="m1.sum", value=3, tags=None),
                MElement(name="m2.mean", value=1, tags={"bat": "baz"}),
                MElement(name="m2.mean", value=2, tags={"foo": "bar"}),
                MElement(name="m1.sum", value=3, tags=None),
                MElement(name="m2.mean", value=1, tags={"foo": "bar"}),
                MElement(name="m1.sum", value=1, tags={"foo": "bar"}),
                MElement(name="m1.sum", value=3, tags={"bat": "baz"}),
                MElement(name="m2.mean", value=2, tags=None),
            ],
        ),
    ]
)
def test_metric_stream_send(init_kwargs, exp_results, test_elements):
    mc = MC(**init_kwargs)
    for te in test_elements:
        mc.send(**te)
        time.sleep(0.01)
    time.sleep(0.1)
    assert all(msink.data == exp_results for msink in mc.msinks)
