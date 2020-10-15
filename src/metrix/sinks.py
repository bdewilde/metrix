import logging

from metrix import MElement


__all__ = ["MSink", "MSinkPrinter", "MSinkLogger", "MSinkTSDB"]


class MSink:
    """
    Base class for subclasses that are called on a :class:`MElement` and
    perform some useful action on it.
    """

    def __call__(self, me: MElement) -> None:
        raise NotImplementedError(
            "every MSink must define its own __call__ method that accepts "
            "a MElement and performs some useful action on it"
        )


class MSinkPrinter(MSink):
    """
    Class that's called on a :class:`MElement` and prints it to stdout. That's it!
    This class is useful in development when experimenting with ``MCoordinator``
    so users can see stream contents, but is not suitable for production.

    .. code-block:: pycon

       >>> from metrix import MElement, MSinkPrinter
       >>> msink = MSinkPrinter()
       >>> msink(MElement("foo", 1))
       MElement(name=foo, value=1, tags=None)
    """

    def __call__(self, me: MElement) -> None:
        print(me)

    def __str__(self):
        return "MSinkPrinter"


class MSinkLogger(MSink):
    """
    Class that's called on a :class:`MElement` and logs it, as-is.

    .. code-block:: pycon

       >>> from metrix import MElement, MSinkLogger
       >>> me = MElement("foo", 1)
       >>> msink = MSinkLogger()
       >>> msink(me)
       INFO:metrix.sinks:MElement(name=foo, value=1, tags=None)
       >>>  msink = MSinkLogger(name="my-logger", level=30, msg_fmt_str="[metric] %s")
       WARNING:my-logger:[metric] MElement(name=foo, value=1, tags=None)

    Args:
        name: Name of the logger to use when logging metric elements.
        level: Level at which metric elements are logged.
        msg_fmt_str: Message format string into which metric elements are merged using
            a string formatting operator. Must contain exactly one "%s" for a given
            :class:`MElement`; may contain any other hard-coded text you wish.

    Attributes:
        logger
        level
        msg_fmt_str
    """

    __slots__ = ("logger", "level", "msg_fmt_str")

    def __init__(
        self,
        name: str = "metrix.sinks",
        level: int = logging.INFO,
        msg_fmt_str: str = "%s",
    ):
        self.logger = logging.getLogger(name)
        self.level = level
        if msg_fmt_str.count("%s") == 1:
            self.msg_fmt_str = msg_fmt_str
        else:
            raise ValueError(
                f"msg_fmt_str='{msg_fmt_str}' is invalid; must contain exactly one "
                "string formatting placeholder for a metric element"
            )

    def __call__(self, me: MElement) -> None:
        self.logger.log(self.level, self.msg_fmt_str, me)

    def __str__(self):
        return f"MSinkLogger(logger={self.logger}, level={self.level})"


class MSinkTSDB(MSink):
    """
    Class that's called on a :class:`MElement` and sends its data to OpenTSDB
    via an instantiated TSDB client.

    Args:
        tsdb_client: Instantiated TSDB client with a ``send`` method, such as
            ``potsdb.Client``.

    Attributes:
        tsdb_client

    Note:
        It's the user's responsibility to ensure that a suitable TSDB client library
        is available in the environment where this class is instantiated.
    """

    def __init__(self, tsdb_client):
        self.tsdb_client = tsdb_client

    def __call__(self, me: MElement) -> None:
        tags = me.tags or {}
        self.tsdb_client.send(me.name, me.value, **tags)

    def __str__(self):
        return f"MSinkTSDB(tsdb_client={self.tsdb_client})"
