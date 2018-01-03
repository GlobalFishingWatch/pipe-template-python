from collections import namedtuple

from pipeline.shard_definition import build_example
from pipeline.objects.namedtuples import _s_to_datetime, _datetime_to_s
from pipeline.objects.feature import Feature
import tensorflow as tf
import numpy as np
import pytest

def fake_feature(i, offset=0):
    args = {'id' : i,
            'timestamp' : _s_to_datetime(i + offset)}
    i += offset
    for name in [
            "timestamp_delta_s", "distance_m", 
            "reported_speed_delta_mps", "reported_course_delta_degrees",
            "implied_speed_delta_mps", "implied_course_delta_degrees",
            "local_time_h", "length_of_day_h", "distance_from_shore_km", 
            "distance_to_prev_anchorage", "distance_to_next_anchorage",
            "time_to_prev_anchorage", "time_to_next_anchorage", "num_neighboring_vessels"]:
            i += 3.3
            args[name] = float(i)
    return Feature(**args)



@pytest.mark.parametrize("base", range(50, 200, 11))
def test_round_trips(base):
    item = (base, [fake_feature(base, i) for i in range(0, 1000, 200)])
    expected = []
    for x in item[1]:
        x = list(x)
        x[1] = _datetime_to_s(x[1])
        expected.append(x[1:])
    expected = np.array(expected)
    serialized = build_example(item)['value']
    deserialized = read_example(serialized, 14 + 1)
    print(deserialized)
    with tf.Session() as sess:
        mmsi = deserialized[0]['mmsi'].eval()
        features = deserialized[1]['movement_features'].eval()
    print mmsi
    print features
    print expected
    assert mmsi == base
    assert np.allclose(expected, features)


def read_example(serialized_example, num_features):
    """ This code is copied from 
    vessel_classifiction/classification/utility/single_feature_file_reader
    and is designed to verify that this is writing code in the same format
    as that expects
  """
    # The serialized example is converted back to actual values.
    context_features, sequence_features = tf.parse_single_sequence_example(
        serialized_example,
        # Defaults are not specified since both keys are required.
        context_features={'mmsi': tf.FixedLenFeature([], tf.int64), },
        sequence_features={
            'movement_features': tf.FixedLenSequenceFeature(
                shape=(num_features, ), dtype=tf.float32)
        })

    return context_features, sequence_features


