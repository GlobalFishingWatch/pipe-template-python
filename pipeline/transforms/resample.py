from __future__ import division
import datetime as dtime
import math
import logging
import pytz
from more_itertools import peekable
from ..objects.location_record import LocationRecord
from .group_by_id import GroupById
from .sort_by_time import SortByTime
from apache_beam import PTransform
from apache_beam import Map


class Resample(PTransform):

    epoch = dtime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)

    def __init__(self, increment_min, max_gap_min):
        self.increment_s = increment_min * 60
        self.max_gap_s = max_gap_min * 60

    def dt_to_s(self, timestamp):
        return (timestamp - self.epoch).total_seconds() 

    def round_to_increment(self, timestamp, rounder):
        return rounder(self.dt_to_s(timestamp) / self.increment_s) * self.increment_s        

    def create_interpolated_record(self, key, t, t0, dt, last_record, current_record):

        assert dt > 0

        mix = (t - t0) / dt

        def interp(name):
            current = getattr(current_record, name)
            last    = getattr(current_record, name)
            return current * mix + last * (1 - mix)

        return LocationRecord(
            id = key,
            timestamp = dtime.datetime.utcfromtimestamp(t).replace(tzinfo=pytz.utc),
            lat = interp('lat'),
            lon = interp('lon'),
            distance_from_shore_km = interp('distance_from_shore_km'), 
            speed_knots = interp('speed_knots'),
            course = interp('course')
            )


    def resample_records(self, records):
        """
        item = (key, records)

        records: list of records that has been sorted by time and uniquified

        """ 
        begin_time = self.round_to_increment(records[0].timestamp, rounder=math.ceil)
        end_time = self.round_to_increment(records[-1].timestamp, rounder=math.floor)
        resampled = []

        if (len(records) >= 2) and (end_time >= begin_time):

            interp_time = begin_time
            record_iter = peekable(records)
            last_record = next(record_iter)
            current_record = next(record_iter)

            while interp_time <= end_time:
                while record_iter and (self.dt_to_s(current_record.timestamp) < interp_time):
                    # Advance record_iter till last_record and current_record bracket current time.
                    last_record = current_record
                    current_record = next(record_iter)
                    assert last_record.id == current_record.id
          
                t0 = self.dt_to_s(last_record.timestamp)
                t1 = self.dt_to_s(current_record.timestamp)
                dt = t1 - t0

                assert t0 <= interp_time
                assert t1 >= interp_time

                if dt < self.max_gap_s:
                    resampled.append(self.create_interpolated_record(
                        key=last_record.id, t=interp_time, t0=t0, dt=dt, 
                        last_record=last_record, current_record=current_record))
                interp_time += self.increment_s
        return resampled

    def resample(self, item):
        key, records = item
        return key, self.resample_records(records)

    def expand(self, xs):
        return (
            xs
            | GroupById()
            | SortByTime()
            | Map(self.resample)
        )

