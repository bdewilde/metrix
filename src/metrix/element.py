from typing import Dict, Optional, Union


Number = Union[int, float]


class MElement:
    """
    An individual metric element -- a single data point -- to be sent through a stream
    and on to one or more sinks.

    .. code-block:: pycon

       >>> from metrix import MElement
       >>> me = MElement("counts", 1, tags={"env": "dev", "foo": "bar"})
       >>> print(me)
       MElement(name=counts, value=1, tags={'env': 'dev', 'foo': 'bar'})
       >>> me.key
       'env:dev|foo:bar'

    Args:
        name: Base name of the metric to which the element belongs.
        value: Numeric value of the metric element.
        tags: Optional tags to associate with this (name, value) pair.

    Note:
        In typical usage, users will not directly instantiate this class; instead,
        they'll pass (name, value, tags) into :meth:`MCoordinator.send()` or
        :meth:`MCoordinator.timer()`, which will create a corresponding
        ``MElement`` under the hood.
    """

    __slots__ = ("name", "value", "tags")

    def __init__(self, name: str, value: Number, *, tags: Optional[Dict] = None):
        self.name = name
        self.value = value
        self.tags = tags

    def __str__(self):
        return f"MElement(name={self.name}, value={self.value}, tags={self.tags})"

    def __eq__(self, other: "MElement") -> bool:
        if self.name == other.name and self.value == other.value and self.tags == other.tags:
            return True
        else:
            return False

    @property
    def key(self) -> Optional[str]:
        return key_from_tags(self.tags)


def key_from_tags(tags: Optional[Dict]) -> Optional[str]:
    """
    Generate a (hashable!) key string from a collection of ``tags``, where tags are
    pipe-delimited and each tag's field and value are colon-delimited.

    .. code-block:: pycon

       >>> key_from_tags({"foo": "bar})
       "foo:bar"
       >>> key_from_tags({"foo": "bar", "bat": "baz"})
       "bat:baz|foo:bar"

    Note:
        The ordering of items in ``tags`` doesn't matter, since the generated key
        is always ordered alphabetically.
    """
    if tags is None:
        return None
    else:
        return "|".join(f"{key}:{val}" for key, val in sorted(tags.items()))


def tags_from_key(key: Optional[str]) -> Optional[Dict]:
    """
    Generate a collection of tags from a key string, where tags are
    pipe-delimited and each tag's field and value are colon-delimited.

    .. code-block:: pycon

       >>> tags_from_key(None)
       None
       >>> tags_from_key("foo:bar")
       {"foo": "bar"}
       >>> tags_from_key("foo:bar|bat:baz")
       {"bat": "baz", "foo": "bar"}

    Note:
        The ordering of items in ``key`` doesn't matter, since the generated tags
        items are always ordered alphabetically.

    See Also:
        :func:`key_from_tags()`
    """
    if key is None:
        return None
    else:
        return dict(keyval.split(":", 1) for keyval in sorted(key.split("|")) if keyval)
