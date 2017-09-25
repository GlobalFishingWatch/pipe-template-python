import datetime as dt
from apache_beam import PTransform
from apache_beam import Map
from apache_beam import Filter

class Sample(PTransform):
    def __init__(self, threshold):
        self.threshold = threshold

    def parse_timestamp(self, x):
        timestamp = dt.datetime.strptime(x['timestamp'], "%Y-%m-%d %H:%M:%S.%f %Z")

        x['timestamp'] = timestamp
        return x

    def is_fishing(self, x):
        return x['score'] > self.threshold

    def expand(self, xs):
        return (
            xs
            | Filter(self.is_fishing)
            | Map(self.parse_timestamp)
        )

