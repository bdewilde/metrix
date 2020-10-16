# metrix

Metrics tracking through streams.

`metrix` is a Python library for tracking metrics through streams, with configurable
tagging, batching, aggregating, and outputting of individual elements. It's designed
for handling multiple metrics collected individually at high rates with shared output
destinations, such as a log file and/or database, especially in the case that outputting
metrics is only required or desired at lower rates.

Full documentation: https://metrix.readthedocs.io
