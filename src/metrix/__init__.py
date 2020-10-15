from metrix.about import __version__
# TODO: declare __version__ here rather than importing about.__version__
# once readthedocs can use sufficiently new version of setuptools
# i'm pretty sure that's the reason builds keep failing :/
# __version__ = "0.1.0"

from metrix.element import MElement
from metrix.sinks import *
from metrix.stream import MStream
from metrix.coordinator import MCoordinator
