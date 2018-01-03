from apache_beam import PTransform
from apache_beam import GroupByKey
from apache_beam import FlatMap
from apache_beam import Map
import logging
from math import sin, cos, atan2
import numpy as np
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
     "d_next_anchorage", "t_next_anchorage", "d_prev_anchorage", "t_prev_anchorage",
     "n_nbrs"])

Latlon = namedtuple("Latlon", ['lat', 'lon'])

# TODO: could use np.nan here and then replace with mean value for this vessel at the end?
DEFAULT_DISTANCE = 10000
DEFAULT_TIME = 30 * 24 * 60 * 60

class CreateFeatures(PTransform):

    @staticmethod
    def _implied_values(records, starts, ends):
        rcd0 = records[0]
        for rcd1 in records[1:]:
            t = mean_time(rcd0.timestamp, rcd1.timestamp)
            lat = mean(rcd0.lat, rcd1.lat)
            lon = mean(rcd0.lon, rcd1.lon)
            vessel_latlon = Latlon(lat, lon)
            reported_speed = mean(rcd0.speed_knots, rcd1.speed_knots)
            reported_course = mean(rcd0.course, rcd1.course)
            vx = 0.5 * (cos(rcd0.course) * rcd0.speed_knots + cos(rcd1.course) * rcd1.speed_knots)
            vy = 0.5 * (sin(rcd0.course) * rcd0.speed_knots + sin(rcd1.course) * rcd1.speed_knots)
            implied_speed = (vx ** 2 + vy ** 2) ** 0.5
            implied_course = atan2(vy, vx)
            d_shore = mean(rcd0.distance_from_shore_km, rcd1.distance_from_shore_km)
            # We search starts to find last, because we want to get current port if we are
            # in port. Vice-versa for next            
            last_i = np.searchsorted(starts[:, 0], t, side='right') - 1
            next_i = np.searchsorted(ends[:, 0], t, side='left') + 1
            # Defaults
            d_prev_anchorage = d_next_anchorage = DEFAULT_DISTANCE
            t_prev_anchorage = t_next_anchorage = DEFAULT_TIME
            if 0 <= last_i < len(starts):
                port_t, port_lat, port_lon = starts[last_i]
                port_latlon = Latlon(port_lat, port_lon)
                d_prev_anchorage = compute_distance_km(vessel_latlon, port_latlon)
                t_prev_anchorage = (t - port_t).total_seconds()
                assert t_prev_anchorage >= 0
            if 0 <= next_i < len(ends):
                port_t, port_lat, port_lon = ends[next_i]
                port_latlon = Latlon(port_lat, port_lon)
                d_next_anchorage = compute_distance_km(vessel_latlon, port_latlon)
                t_next_anchorage = (port_t - t).total_seconds()
                assert t_next_anchorage >= 0        
            #TODO: implement when we have neighbor counts
            n_nbrs = 0
            yield Implied(t, lat, lon, reported_speed, reported_course,
                         implied_speed, implied_course, d_shore, d_next_anchorage, 
                         t_next_anchorage, d_prev_anchorage, t_prev_anchorage, n_nbrs)
            rcd0 = rcd1


    def create_features(self, item):
        # Because we cogroup on something already grouped, we get [records]
        # Why second level though? TODO: figure out
        vessel_id, (raw_records, port_visits) = item  
        raw_records = list(raw_records)
        if not raw_records:
            return           
        records = list(raw_records[0])
        logging.info("RECORDS: 0  %s", type(records))
        logging.info("RECORDS: 1 %s", len(records))
        records.sort(key = lambda x: x.timestamp) 
        if len(records) < 3:
            return
        if len(port_visits):
            starts = [(x.start_timestamp, x.start_lat, x.start_lon) for x in port_visits]
            starts.sort()
            starts = np.array(starts)
            ends = [(x.end_timestamp, x.end_lat, x.end_lon) for x in port_visits]
            ends.sort()
            ends = np.array(ends)
        else:
            starts = ends = np.zeros([0, 3], dtype=float)
        logging.info("STARTS: %s %s", starts.shape, ends.shape)

        implied = iter(self._implied_values(records, starts, ends))
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
                distance_to_next_anchorage    = mean(imp0.d_next_anchorage, imp1.d_next_anchorage),
                time_to_next_anchorage        = mean(imp0.t_next_anchorage, imp1.t_next_anchorage),
                distance_to_prev_anchorage    = mean(imp0.d_prev_anchorage, imp1.d_prev_anchorage),
                time_to_prev_anchorage        = mean(imp0.t_prev_anchorage, imp1.t_prev_anchorage),
                num_neighboring_vessels       = mean(imp0.n_nbrs, imp1.n_nbrs),
                )
            imp0 = imp1


    def create_tagged_features(self, item):
        vessel_id, records = item
        return list(self.create_features(item))

    def expand(self, xs):
        return (
            xs
            | FlatMap(self.create_tagged_features)
        )


