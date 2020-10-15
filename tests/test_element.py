import pytest

import metrix.element
from metrix import MElement


@pytest.mark.parametrize(
    "tags,key",
    [
        ({"foo": "bar"}, "foo:bar"),
        ({"foo": "bar", "bat": "baz"}, "bat:baz|foo:bar"),
        ({"bat": "baz", "foo": "bar"}, "bat:baz|foo:bar"),
        ({}, ""),
        (None, None),
    ]
)
def test_key_from_tags(tags, key):
    assert metrix.element.key_from_tags(tags) == key


@pytest.mark.parametrize(
    "key,tags",
    [
        ("foo:bar", {"foo": "bar"}),
        ("bat:baz|foo:bar", {"bat": "baz", "foo": "bar"}),
        ("foo:bar|bat:baz", {"bat": "baz", "foo": "bar"}),
        ("", {}),
        (None, None),
    ]
)
def test_tags_from_key(key, tags):
    assert metrix.element.tags_from_key(key) == tags


@pytest.mark.parametrize(
    "name,value,tags",
    [
        ("metric", 1, None),
        ("metric", 2, {"foo": "bar"})
    ]
)
def test_metric_element(name, value, tags):
    me = MElement(name, value, tags=tags)
    assert all(hasattr(me, attr) for attr in ["name", "value", "tags", "key"])
    assert me.key == metrix.element.key_from_tags(tags)


@pytest.mark.parametrize(
    "me1,me2,is_equal",
    [
        (MElement("foo", 1), MElement("foo", 1), True),
        (MElement("bar", 2), MElement("bar", 2), True),
        (MElement("foo", 1), MElement("bar", 1), False),
        (MElement("foo", 1), MElement("foo", 2), False),
        (MElement("foo", 1), MElement("foo", 1.0), True),
        (MElement("foo", 1), MElement("FOO", 1), False),
        (
            MElement("foo", 1, tags={"bat": "baz"}),
            MElement("foo", 1, tags={"bat": "baz"}),
            True,
        ),
        (MElement("foo", 1, tags=None), MElement("foo", 1, tags={"bat": "baz"}), False),
        (
            MElement("foo", 1, tags={"bat": "baz", "spam": "eggs"}),
            MElement("foo", 1, tags={"spam": "eggs", "bat": "baz"}),
            True,
        ),
    ]
)
def test_metric_element_eq(me1, me2, is_equal):
    assert (me1 == me2) is is_equal
