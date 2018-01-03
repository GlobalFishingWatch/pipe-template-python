from collections import namedtuple
from .namedtuples import NamedtupleCoder

PortVisit = namedtuple("PortVisit", ["vessel_id", 
            "start_timestamp", "start_lat", "start_lon",
            "end_timestamp", "end_lat", "end_lon"])

class PortVisitCoder(NamedtupleCoder):
    target = PortVisit
    time_fields = ['start_timestamp', 'end_timestamp']


PortVisitCoder.register()





