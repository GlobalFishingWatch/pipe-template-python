from collections import namedtuple

from pipeline.transforms.trim_stationary_periods import TrimStationaryPeriods
from pipeline.objects.namedtuples import _s_to_datetime, _datetime_to_s

Record = namedtuple("Record", ['timestamp', 'lat', 'lon'])

def rcd(hours, lat_minutes, lon_minutes):
    return Record(_s_to_datetime(hours * 60 * 60), lat_minutes / 60.0, lon_minutes / 60.0)

steady_sample_1 = [
    rcd(i, 0, i * 1.0) for i in range(100)
]

stopped_sample = [
    rcd(i + 100, 0, 1.0 + steady_sample_1[-1].lon * 60.0) for i in range(200)
]

steady_sample_2 = [
    rcd(i + 300, 0, i * 1.0 + 1.0 + stopped_sample[-1].lon * 60.0) for i in range(100)
]

# When combined, stopped sample is all at same location, while other two are steadily moving
# before that.
combined_sample = steady_sample_1 + stopped_sample + steady_sample_2


trimmer = TrimStationaryPeriods(max_distance_km=0.8, min_period_minutes=60*48)


def test_trim_1():
    assert list(trimmer.trim(steady_sample_1)) == steady_sample_1

def test_trim_2():
    assert list(trimmer.trim(steady_sample_2)) == steady_sample_2

def test_trim_3():
    assert list(trimmer.trim(stopped_sample)) == stopped_sample[:49] + stopped_sample[200-49:]

def test_trim_4():
    assert list(trimmer.trim(combined_sample)) == combined_sample[:149] + combined_sample[300-49:]

