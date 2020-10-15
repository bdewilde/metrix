import logging
from unittest.mock import MagicMock

import pytest

import metrix.sinks
from metrix import MElement


@pytest.fixture(scope="module")
def melement():
    return MElement("metric", 1, tags={"foo": "bar"})


def test_msink_printer(melement):
    msink = metrix.sinks.MSinkPrinter()
    msink(melement)  # assert not raises, effectively
    # TODO: is there really no way to use `capsys` fixture to get the print statement
    # arising from this call? afaict it only captures prints *in* the test function


@pytest.mark.parametrize(
    "init_kwargs",
    [
        {"name": "metrix.sinks", "level": logging.INFO, "msg_fmt_str": "%s"},
        {"name": "foo", "level": 40},
        {
            "name": "my-great-logger",
            "level": logging.WARNING,
            "msg_fmt_str": "[foo][metric] my great logging stmt: %s",
        },
        {},
    ],
)
def test_msink_logger(init_kwargs, caplog):
    if init_kwargs.get("level"):
        caplog.set_level(init_kwargs["level"])
    msink = metrix.sinks.MSinkLogger(**init_kwargs)
    msink(melement)  # assert not raises, effectively
    assert all(hasattr(msink, attr) for attr in ["logger", "level", "msg_fmt_str"])
    assert msink.logger.name == init_kwargs.get("name", "metrix.sinks")
    if init_kwargs.get("level"):
        assert msink.level == init_kwargs["level"]
        assert str(melement) in caplog.text
    if init_kwargs.get("msg_fmt_str"):
        msg_fmt_str = init_kwargs["msg_fmt_str"]
        assert msink.msg_fmt_str == msg_fmt_str
        if len(msg_fmt_str) > 2:
            assert msg_fmt_str[:msg_fmt_str.index("%s")] in caplog.text


@pytest.mark.parametrize(
    "msg_fmt_str", ["", "%s %s", "foo %s bar %s bat %s baz %s"]
)
def test_msink_logger_bad_init(msg_fmt_str):
    with pytest.raises(ValueError):
        _ = metrix.sinks.MSinkLogger(msg_fmt_str=msg_fmt_str)


def test_msink_tsdb(melement):
    mock_tsdb_client = MagicMock()
    msink = metrix.sinks.MSinkTSDB(mock_tsdb_client)
    assert msink.tsdb_client is mock_tsdb_client
    msink(melement)  # assert not raises, effectively
    mock_tsdb_client.send.assert_called_once_with(
        melement.name, melement.value, **melement.tags
    )
