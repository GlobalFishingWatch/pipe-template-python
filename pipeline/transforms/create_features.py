from apache_beam import PTransform
from apache_beam import GroupByKey
from apache_beam import FlatMap
from apache_beam import Map
from math import sin, cos, atan2
from datetime import timedelta, datetime
from collections import namedtuple
from pipeline.utils.distance import distance as compute_distance_km
from pipeline.objects.feature import Feature


def mean(x0, x1):
    return 0.5 * (x0 + x1)

def mean_time(t0, t1):
    return t0 + timedelta(seconds=0.5 * (t1 - t0).total_seconds())

Implied = namedtuple("Implied",
    ["t", "lat", "lon", "reported_speed", "reported_course",
    "implied_speed", "implied_course", "d_shore", 
     "d_next_anchorage", "t_next_anchorage", "d_prev_anchorage", "t_prev_anchorage"])

class CreateFeatures(PTransform):

    @staticmethod
    def _implied_values(records):
        rcd0 = records[0]
        for rcd1 in records[1:]:
            t = mean_time(rcd0.timestamp, rcd1.timestamp)
            lat = mean(rcd0.lat, rcd1.lat)
            lon = mean(rcd0.lon, rcd1.lon)
            reported_speed = mean(rcd0.speed_knots, rcd1.speed_knots)
            reported_course = mean(rcd0.course, rcd1.course)
            vx = 0.5 * (cos(rcd0.course) * rcd0.speed_knots + cos(rcd1.course) * rcd1.speed_knots)
            vy = 0.5 * (sin(rcd0.course) * rcd0.speed_knots + sin(rcd1.course) * rcd1.speed_knots)
            implied_speed = (vx ** 2 + vy ** 2) ** 0.5
            implied_course = atan2(vy, vx)
            d_shore = mean(rcd0.distance_from_shore_km, rcd1.distance_from_shore_km)
            # TODO: implement once we have tagged values
            d_next_anchorage = 0
            t_next_anchorage = 0
            d_prev_anchorage = 0
            t_prev_anchorage = 0
            yield Implied(t, lat, lon, reported_speed, reported_course,
                         implied_speed, implied_course, d_shore, d_next_anchorage, 
                         t_next_anchorage, d_prev_anchorage, t_prev_anchorage)


    def create_features(self, item):
        vessel_id, records = item
        records = list(records)
        records.sort(key = lambda x: x.timestamp) # TODO: remove once this is done in the thinning step
        if len(records) < 3:
            return
        implied = iter(self._implied_values(records))
        imp0 = implied.next()
        for imp1 in implied:
            lat = mean(imp0.lat, imp1.lat)
            lon = mean(imp0.lon, imp1.lon)
            timestamp = mean_time(imp0.t, imp1.t)
            hour_of_day = timestamp.hour + timestamp.minute / 60.
            hour_offset = lat / 360.0 * 24.0
            assert isinstance(timestamp, datetime), "expected datetime object, got {}".format(type(timestamp))
            yield Feature(
                id = vessel_id,
                timestamp                     = timestamp,
                timestamp_delta_s             = (imp1.t - imp0.t).total_seconds(),
                distance_m                    = compute_distance_km(imp1, imp0) * 1000,
                reported_speed_delta_mps      = imp1.reported_speed  - imp0.reported_speed,
                reported_course_delta_degrees = imp1.reported_course - imp0.reported_course,
                implied_speed_delta_mps       = imp1.implied_speed   - imp0.implied_speed,
                implied_course_delta_degrees  = imp1.implied_course  - imp0.implied_course,
                local_time_h                  = (hour_of_day - hour_offset) % 24,
                length_of_day_h               = 0, # TODO: implement as function of lat, timestamp
                # TODO, maybe just use existing distance to port
                distance_from_shore_km        = mean(imp0.d_shore, imp1.d_shore),
                distance_to_next_anchorage              = mean(imp0.d_next_anchorage, imp1.d_next_anchorage),
                time_to_next_anchorage              = mean(imp0.t_next_anchorage, imp1.t_next_anchorage),
                distance_to_prev_anchorage              = mean(imp0.d_prev_anchorage, imp1.d_prev_anchorage),
                time_to_prev_anchorage              = mean(imp0.t_prev_anchorage, imp1.t_prev_anchorage))
            imp0 = imp1


    def create_tagged_features(self, item):
        vessel_id, records = item
        return list(self.create_features(item))

    def expand(self, xs):
        return (
            xs
            | FlatMap(self.create_tagged_features)
        )


