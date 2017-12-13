from collections import namedtuple

# from pipeline.shard_definition import ProtbufCoder
from pipeline.shard_definition import build_examples
from pipeline.objects.namedtuples import _s_to_datetime, _datetime_to_s
from pipeline.objects.feature import Feature
import tensorflow as tf
import numpy as np

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


# def test_build_examples():
#     base = 57
#     item = (base, [fake_feature(base, i) for i in range(0, 1000, 200)])
#     assert build_examples(item) == {'id': 57, 'value': '\n\x86\x05\n\r\n\x04mmsi\x12\x05\x1a\x03\n\x019\n\xf4\x04\n\x11movement_features\x12\xde\x04\x12\xdb\x04\n\xd8\x04\x00\x00dB33qBff~B\xcd\xcc\x85Bff\x8cB\x00\x00\x93B\x9a\x99\x99B33\xa0B\xcd\xcc\xa6Bff\xadB\x00\x00\xb4B\x9a\x99\xbaB33\xc1B\xcd\xcc\xc7Bff\xceB\x00\x00hB33uB33\x81B\xcd\xcc\x87Bff\x8eB\x00\x00\x95B\x9a\x99\x9bB33\xa2B\xcd\xcc\xa8Bff\xafB\x00\x00\xb6B\x9a\x99\xbcB33\xc3B\xcd\xcc\xc9Bff\xd0B\x00\x00lB33yB33\x83B\xcd\xcc\x89Bff\x90B\x00\x00\x97B\x9a\x99\x9dB33\xa4B\xcd\xcc\xaaBff\xb1B\x00\x00\xb8B\x9a\x99\xbeB33\xc5B\xcd\xcc\xcbBff\xd2B\x00\x00pB33}B33\x85B\xcd\xcc\x8bBff\x92B\x00\x00\x99B\x9a\x99\x9fB33\xa6B\xcd\xcc\xacBff\xb3B\x00\x00\xbaB\x9a\x99\xc0B33\xc7B\xcd\xcc\xcdBff\xd4B\x00\x00tB\x9a\x99\x80B33\x87B\xcd\xcc\x8dBff\x94B\x00\x00\x9bB\x9a\x99\xa1B33\xa8B\xcd\xcc\xaeBff\xb5B\x00\x00\xbcB\x9a\x99\xc2B33\xc9B\xcd\xcc\xcfBff\xd6B\x00\x00xB\x9a\x99\x82B33\x89B\xcd\xcc\x8fBff\x96B\x00\x00\x9dB\x9a\x99\xa3B33\xaaB\xcd\xcc\xb0Bff\xb7B\x00\x00\xbeB\x9a\x99\xc4B33\xcbB\xcd\xcc\xd1Bff\xd8B\x00\x00|B\x9a\x99\x84B33\x8bB\xcd\xcc\x91Bff\x98B\x00\x00\x9fB\x9a\x99\xa5B33\xacB\xcd\xcc\xb2Bff\xb9B\x00\x00\xc0B\x9a\x99\xc6B33\xcdB\xcd\xcc\xd3Bff\xdaB\x00\x00\x80B\x9a\x99\x86B33\x8dB\xcd\xcc\x93Bff\x9aB\x00\x00\xa1B\x9a\x99\xa7B33\xaeB\xcd\xcc\xb4Bff\xbbB\x00\x00\xc2B\x9a\x99\xc8B33\xcfB\xcd\xcc\xd5Bff\xdcB\x00\x00\x82B\x9a\x99\x88B33\x8fB\xcd\xcc\x95Bff\x9cB\x00\x00\xa3B\x9a\x99\xa9B33\xb0B\xcd\xcc\xb6Bff\xbdB\x00\x00\xc4B\x9a\x99\xcaB33\xd1B\xcd\xcc\xd7Bff\xdeB\x00\x00\x84B\x9a\x99\x8aB33\x91B\xcd\xcc\x97Bff\x9eB\x00\x00\xa5B\x9a\x99\xabB33\xb2B\xcd\xcc\xb8Bff\xbfB\x00\x00\xc6B\x9a\x99\xccB33\xd3B\xcd\xcc\xd9Bff\xe0B'}


# TODO use parameterize instead
def test_round_trips():
    for base in range(50, 200, 11):
        item = (base, [fake_feature(base, i) for i in range(0, 1000, 200)])
        expected = []
        for x in item[1]:
            x = list(x)
            x[1] = _datetime_to_s(x[1])
            expected.append(x[1:])
        expected = np.array(expected)
        serialized = build_examples(item)['value']
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


# def test_encoder():
#     feature = fake_feature(57)
#     munged = {'id': feature[0], 'values': feature[1:]}
#     assert ProtbufCoder().encode(munged) == "\nf\n\r\n\x04mmsi\x12\x05\x1a\x03\n\x019\nU\n\x11movement_features\x12@\x12>\n<\x00\x00dB33qBff~B\xcd\xcc\x85Bff\x8cB\x00\x00\x93B\x9a\x99\x99B33\xa0B\xcd\xcc\xa6Bff\xadB\x00\x00\xb4B\x9a\x99\xbaB33\xc1B\xcd\xcc\xc7Bff\xceB"


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


