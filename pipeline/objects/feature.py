from collections import namedtuple
from .namedtuples import NamedtupleCoder

Feature = namedtuple("Feature",
    ["id", "timestamp", 
        "timestamp_delta_s", "distance_m", 
        "reported_speed_delta_mps", "reported_course_delta_degrees",
        "implied_speed_delta_mps", "implied_course_delta_degrees",
        "local_time_h", "length_of_day_h", "distance_from_shore_km", 
        "distance_to_prev_anchorage", "distance_to_next_anchorage",
        "time_to_prev_anchorage", "time_to_next_anchorage", "num_neighboring_vessels"])
# IMPORTANT: ID must be first feature
assert Feature._fields[0] == "id"

class FeatureCoder(NamedtupleCoder):
    target = Feature
    time_fields = ['timestamp']


FeaturesFromTuples, FeatureFromDicts, FeaturesToDicts = FeatureCoder.register()





