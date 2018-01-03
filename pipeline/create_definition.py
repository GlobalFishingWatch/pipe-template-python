from pipeline.transforms.source import Source
from pipeline.transforms.group_by_id import GroupById
from pipeline.transforms.resample import Resample
from pipeline.transforms.create_features import CreateFeatures
from pipeline.transforms.trim_stationary_periods import TrimStationaryPeriods
from pipeline.transforms.sink import Sink
from pipeline.objects.location_record import LocationRecord
from pipeline.objects.feature import Feature
from pipeline.objects.port_visit import PortVisit
from pipeline.objects.namedtuples import _s_to_datetime
from pipeline.options.create_features_options import CreateFeaturesOptions
from pipeline.schemas.output import build as build_output_schema
from apache_beam import io
from apache_beam.pvalue import AsList
from apache_beam import CoGroupByKey
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import PipelineState
from apache_beam.transforms.window import TimestampedValue
from pipe_tools.io import WriteToBigQueryDatePartitioned
from collections import namedtuple
import datetime
import logging




def create_queries(options, start_date, end_date):
    create_options = options.view_as(CreateFeaturesOptions)
    template = """
    SELECT
      FLOAT(TIMESTAMP_TO_MSEC(timestamp)) / 1000  AS timestamp,
      STRING(mmsi)               AS id,
      lat                        AS lat,
      lon                        AS lon,
      speed                      AS speed_knots,
      course                     AS course,
      distance_from_shore / 1000 AS distance_from_shore_km
    FROM
      TABLE_DATE_RANGE([{table}.], 
                            TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}'))
    WHERE
      lat   IS NOT NULL AND lat >= -90.0 AND lat <= 90.0 AND
      lon   IS NOT NULL AND lon >= -180.0 AND lon <= 180.0 AND
      speed IS NOT NULL AND speed >=0 AND speed <= 100.0 AND
      course IS NOT NULL AND course >= 0 AND course < 360 AND
      distance_from_shore IS NOT NULL AND distance_from_shore >= 0 AND distance_from_shore <= 20000.0
    """
    if create_options.fast_test:
        template += 'LIMIT 100000'
    # Pad start date by one day to allow warm up.
    start = start_date - datetime.timedelta(days=1)
    return LocationRecord.create_queries(create_options.source_table, start, end_date, template)



VISIT_PADDING_DAYS = 30

def create_visit_queries(options, start_date, end_date):
    create_options = options.view_as(CreateFeaturesOptions)
    template = """
    SELECT
      vessel_id,
      FLOAT(TIMESTAMP_TO_MSEC(start_timestamp)) / 1000  AS start_timestamp,
      start_lat,
      start_lon,
      FLOAT(TIMESTAMP_TO_MSEC(end_timestamp)) / 1000  AS end_timestamp,
      end_lat,
      end_lon
    FROM
      TABLE_DATE_RANGE([{table}], 
                            TIMESTAMP('{start:%Y-%m-%d}'), TIMESTAMP('{end:%Y-%m-%d}'))
    """
    # Pad start date to capture long visits vessels are sharded by start date.
    start = start_date - datetime.timedelta(days=VISIT_PADDING_DAYS)
    return PortVisit.create_queries(create_options.visits_table, start, end_date, template)


def create_tagged_visit(x):
    x['start_timestamp'] = _s_to_datetime(x['start_timestamp'])
    x['end_timestamp'] = _s_to_datetime(x['end_timestamp'])
    return (x['vessel_id'], PortVisit(**x))


def run(options):

    p = Pipeline(options=options)

    cloud_options = options.view_as(GoogleCloudOptions)
    create_options = options.view_as(CreateFeaturesOptions)

    sink = WriteToBigQueryDatePartitioned(
        temp_gcs_location=cloud_options.temp_location,
        table=create_options.sink_table,
        write_disposition="WRITE_TRUNCATE",
        schema=build_output_schema()
        )

    start_date = datetime.datetime.strptime(create_options.start_date, '%Y-%m-%d')
    end_date= datetime.datetime.strptime(create_options.end_date, '%Y-%m-%d') 


    sources = [(p | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x)))
                    for (i, x) in enumerate(create_queries(options, start_date, end_date))]

    visit_sources = [(p | "Read_visits_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x)))
                    for (i, x) in enumerate(create_visit_queries(options, start_date, end_date))]


    # Other inputs: 
    #  - nbr_counts (from encounters_table)
    #  - Port events: from port (events table)
    #  - Cogroup by key on vessel_id, then simply add.
    vessel_locations = (sources
        | "FlattenLocations" >> Flatten()
        | LocationRecord.FromDict()
        | Resample(increment_min=15, max_gap_min=120)
        | TrimStationaryPeriods(max_distance_km=0.8, min_period_minutes=60*48)
        )

    port_visits = (visit_sources 
        | "FlattenVists" >> Flatten()
        | Map(create_tagged_visit)
        )
        
    ((vessel_locations, port_visits)
        | CoGroupByKey()
        | CreateFeatures() # start_date, end_date) #TODO: trim by start, end dates
        | Feature.ToDict()
        | Map(lambda x: TimestampedValue(x, x['timestamp']))
        | "WriteToSink" >> sink
    )

    result = p.run()

    success_states = set([PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN])

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1