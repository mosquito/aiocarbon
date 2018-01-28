from collections import namedtuple

import time


class Metric(namedtuple("Metric", ("name", "value", "timestamp"))):
    def __new__(cls, name, value, timestamp=None):
        timestamp = timestamp or time.time()
        return super().__new__(cls, name=name, value=value, timestamp=timestamp)

    def __str__(self):
        return "{0.name} {0.value} {0.timestamp}".format(self)

    def __repr__(self):
        return "<Metric: {0.name} {0.value} {0.timestamp}>".format(self)
