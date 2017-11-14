from apache_beam import PTransform
from apache_beam import GroupByKey
from apache_beam import Map
from apache_beam import FlatMap
from math import sin, cos, atan2
from datetime import timedelta, datetime
from collections import namedtuple
from pipeline.utils.distance import distance as compute_distance_km
from pipeline.objects.feature import Feature
import numpy as np

class TrimStationaryPeriods(PTransform):

    def __init__(self, max_distance_km, min_period_minutes):
        self.max_distance_km = max_distance_km
        self.min_period = timedelta(minutes=min_period_minutes)

    def trim(self, records):
        records = iter(records)
        next_rcd = anchor = next(records)
        cluster = [anchor]
        done = False
        while next_rcd:
            while True:
                try:
                    rcd = next(records)
                except StopIteration:
                    next_rcd = None
                    break
                dist = compute_distance_km(rcd, anchor)
                if dist > self.max_distance_km:
                    next_rcd = rcd
                    break
                else:
                    cluster.append(rcd)

            assert anchor is not None
            assert not [x for x in cluster if (x is None)]
            assert anchor == cluster[0], (anchor, cluster[0])
            dts = [(x.timestamp - anchor.timestamp) for x in cluster]
            if dts[-1] <= self.min_period:
                for x in cluster:
                    yield x
            else:
                i1 = np.searchsorted(dts, self.min_period, side='right')
                i2 = np.searchsorted(dts, dts[-1] - self.min_period, side='left')
                i2 = max(i2, i1) # Ensure we don't duplicate any points
                for x in cluster[:i1]:
                    yield x
                for x in cluster[i2:]:
                    yield x


            anchor = next_rcd
            cluster = [next_rcd]
        assert next_rcd is None


    def trim_stationary_periods(self, item):
        vessel_id, records = item
        return vessel_id, list(self.trim(records))

    def expand(self, xs):
        return (xs
            | Map(self.trim_stationary_periods)
            )

