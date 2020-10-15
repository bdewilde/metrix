import statistics
import time

import pytest
import streamz

import metrix.stream
from metrix import MElement, MStream


@pytest.fixture(scope="module")
def test_elements():
    return [
        {"value": 1, "tags": None},
        {"value": 2, "tags": {"foo": "bar"}},
        {"value": 3, "tags": None},
        {"value": 1, "tags": {"bat": "baz"}},
        {"value": 2, "tags": {"foo": "bar"}},
        {"value": 3, "tags": None},
        {"value": 1, "tags": {"foo": "bar"}},
        {"value": 1, "tags": {"foo": "bar"}},
        {"value": 3, "tags": {"bat": "baz"}},
        {"value": 2, "tags": None},
    ]


@pytest.mark.parametrize(
    "group,exp",
    [[(1, 2, 3), True], [tuple(), False], [(MElement("metric", 1),), True]],
)
def test_group_has_elements(group, exp):
    assert metrix.stream.group_has_elements(group) is exp


@pytest.mark.parametrize(
    "init_kwargs",
    [
        {"name": "metric", "agg": sum, "window_size": 1},
        {"name": "metric", "agg": [min, max], "batch_size": 10},
        {"name": "metric", "agg": {"avg": statistics.mean, "std": statistics.stdev}, "batch_size": 5},
        {"name": "metric", "agg": sum, "default_tags": None, "window_size": 1},
        {"name": "metric", "agg": sum, "default_tags": {"foo": "bar"}, "window_size": 1},
        {"name": "metric", "agg": sum, "default_tags": None, "batch_size": 1},
    ]
)
def test_metric_stream_init(init_kwargs):
    mstream = MStream(**init_kwargs)
    assert hasattr(mstream, "source") and isinstance(mstream.source, streamz.Source)
    assert hasattr(mstream, "stream") and isinstance(mstream.stream, streamz.Stream)
    assert hasattr(mstream, "name") and mstream.name == init_kwargs["name"]


@pytest.mark.parametrize(
    "init_kwargs",
    [
        {"name": "metric", "agg": sum, "window_size": None, "batch_size": None},
        {"name": "metric", "agg": sum, "window_size": 1, "batch_size": 1},
        {"name": "metric", "agg": sum, "window_size": -1},
        {"name": "metric", "agg": sum, "batch_size": -1},
        {"name": "metric", "agg": "sum", "batch_size": 1},
        {"name": "metric", "agg": None, "batch_size": 1},
    ]
)
def test_metric_stream_bad_init(init_kwargs):
    with pytest.raises((ValueError, TypeError)):
        mstream = MStream(**init_kwargs)


@pytest.mark.parametrize("name", ["foo", "bar"])
def test_metric_stream_str(name):
    mstream = MStream(name, agg=sum, batch_size=1)
    assert str(mstream) and name in str(mstream)


@pytest.mark.parametrize(
    "init_kwargs,exp_results,wait_time",
    [
        (
            {"name": "metric", "agg": sum, "default_tags": None, "window_size": 0.4},
            [
                MElement("metric.sum", 9, tags=None),
                MElement("metric.sum", 6, tags={"foo": "bar"}),
                MElement("metric.sum", 4, tags={"bat": "baz"})
            ],
            0.3,
        ),
        (
            {"name": "metric", "agg": sum, "default_tags": None, "batch_size": 5},
            [
                MElement("metric.sum", 4, tags=None),
                MElement("metric.sum", 4, tags={"foo": "bar"}),
                MElement("metric.sum", 1, tags={"bat": "baz"}),
                MElement("metric.sum", 5, tags=None),
                MElement("metric.sum", 2, tags={"foo": "bar"}),
                MElement("metric.sum", 3, tags={"bat": "baz"})
            ],
            0.1,
        ),
        (
            {"name": "metric", "agg": [min, max], "default_tags": None, "batch_size": 10},
            [
                MElement("metric.min", 1, tags=None),
                MElement("metric.min", 1, tags={"foo": "bar"}),
                MElement("metric.min", 1, tags={"bat": "baz"}),
                MElement("metric.max", 3, tags=None),
                MElement("metric.max", 2, tags={"foo": "bar"}),
                MElement("metric.max", 3, tags={"bat": "baz"})
            ],
            0.1,
        ),
        (
            {"name": "m", "agg": {"avg": statistics.mean}, "default_tags": {"foo": "bar"}, "batch_size": 10},
            [
                MElement("m.avg", 1.875, tags={"foo": "bar"}),
                MElement("m.avg", 2, tags={"bat": "baz", "foo": "bar"}),
            ],
            0.1,
        ),
        (
            {"name": "metric", "agg": sum, "default_tags": {"foo": "BAR"}, "batch_size": 10},
            [
                MElement("metric.sum", 9, tags={"foo": "BAR"}),
                MElement("metric.sum", 6, tags={"foo": "bar"}),
                MElement("metric.sum", 4, tags={"bat": "baz", "foo": "BAR"}),
            ],
            0.1,
        ),
    ]
)
def test_metric_stream_send(init_kwargs, exp_results, wait_time, test_elements):
    mstream = MStream(**init_kwargs)
    # hack! sink stream outputs to a list
    # normally, we'd use a MSink w/ MCoordinator to handle this
    obs_results = mstream.stream.sink_to_list()
    for te in test_elements:
        mstream.send(te["value"], tags=te["tags"])
        time.sleep(0.02)
    # total time to send all metrics is approx 10 * 0.02 = 0.2 seconds
    # so we may need to wait a bit more time before window(s) finish
    time.sleep(wait_time)
    assert obs_results == exp_results


@pytest.mark.parametrize(
    "init_kwargs,timer_kwargs,exp_results",
    [
        (
            {"name": "metric", "agg": sum, "default_tags": None, "batch_size": 1},
            {"scale": 1, "tags": None},
            [("metric.sum", 0.1, None)],
        ),
        (
            {"name": "metric", "agg": sum, "default_tags": None, "batch_size": 1},
            {"scale": 1000, "tags": None},
            [("metric.sum", 100.0, None)],
        ),
        (
            {"name": "metric", "agg": sum, "default_tags": None, "batch_size": 1},
            {"scale": 1, "tags": {"foo": "bar"}},
            [("metric.sum", 0.1, {"foo": "bar"})],
        ),
        (
            {"name": "metric", "agg": sum, "default_tags": {"foo": "bar"}, "batch_size": 1},
            {"scale": 1, "tags": None},
            [("metric.sum", 0.1, {"foo": "bar"})],
        ),
        (
            {"name": "metric", "agg": sum, "default_tags": {"foo": "bar"}, "batch_size": 1},
            {"scale": 1, "tags": {"bat": "baz"}},
            [("metric.sum", 0.1, {"bat": "baz", "foo": "bar"})],
        ),
        (
            {"name": "metric", "agg": sum, "default_tags": {"foo": "bar"}, "batch_size": 1},
            {"scale": 1, "tags": {"foo": "BAR"}},
            [("metric.sum", 0.1, {"foo": "BAR"})],
        ),
        (
            {"name": "metric", "agg": [min, max], "default_tags": None, "batch_size": 1},
            {"scale": 1, "tags": None},
            [("metric.min", 0.1, None), ("metric.max", 0.1, None)],
        ),
    ]
)
def test_metric_stream_timer(init_kwargs, timer_kwargs, exp_results):
    mstream = MStream(**init_kwargs)
    # hack! sink stream outputs to a list
    # normally, we'd use a MSink w/ MCoordinator to handle this
    obs_results = mstream.stream.sink_to_list()
    with mstream.timer(**timer_kwargs):
        time.sleep(0.1)
    time.sleep(0.2)
    assert len(obs_results) == len(exp_results)
    assert all(
        (
            obs_result.name == exp_result[0] and
            obs_result.value == pytest.approx(exp_result[1], rel=1.0) and
            obs_result.tags == exp_result[2]
        )
        for obs_result, exp_result in zip(obs_results, exp_results)
    )
