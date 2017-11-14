from collections import namedtuple
from .namedtuples import NamedtupleCoder

LocationRecord = namedtuple("LocationRecord",
    ['id', 'timestamp', 'lat', 'lon', 'distance_from_shore_km', 'speed_knots', 'course'])


class LocationRecordCoder(NamedtupleCoder):
    target = LocationRecord
    time_fields = ['timestamp']


LocationRecordsFromTuples, LocationRecordsFromDicts, LocationRecordsToDicts = LocationRecordCoder.register()





