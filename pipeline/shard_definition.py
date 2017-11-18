import os
from pipeline.transforms.source import Source
from pipeline.transforms.byte_encode import ByteEncode
from pipeline.transforms.group_by_id import GroupById
from pipe_tools.io.keypartitionedsink import WriteToKeyPartitionedFiles
from pipeline.objects.location_record import LocationRecordsFromDicts
from pipeline.objects.feature import Feature
from pipeline.objects.feature import FeaturesToDicts
from pipeline.schemas.output import build as build_output_schema
import apache_beam as beam
from apache_beam import io
from apache_beam.pvalue import AsList
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam.transforms.window import TimestampedValue

import datetime


class ByteCoder(beam.coders.Coder):
    """A coder used for writing features"""

    def encode(self, value):
        return value['value']
        # return str(value['timestamp'])

    def decode(self, value):
        raise NotImplementedError

    def is_deterministic(self):
        return True


class PipelineDefinition():

    def __init__(self, options):
        self.options = options

    def build(self, pipeline):

        # WriteToIdPartitionedFiles mangles the given path, using the last item as the filename,
        # and inserting a directory named after the id before the path
        shard_pseudo_path = os.path.join(self.options.shard_location, 'features', 'shard')

        id_list_path = os.path.join(self.options.shard_location, 'idlist.txt')



        start_date = datetime.datetime.strptime(self.options.start_date, '%Y-%m-%d')
        end_date = datetime.datetime.strptime(self.options.end_date, '%Y-%m-%d')

        sources = [(pipeline | "Read_{}".format(i) >> io.Read(io.gcp.bigquery.BigQuerySource(query=x)))
                        for (i, x) in enumerate(
                            Feature.create_queries(self.options.sink_table, start_date, end_date))]

        print(self.options.sink_table)
        print(list(Feature.create_queries(self.options.sink_table, start_date, end_date))[0])

        features = (sources
            | Flatten()
            | Feature.FromDict()
        )

        (features
            | ByteEncode() 
            | WriteToKeyPartitionedFiles('id', shard_pseudo_path, coder=ByteCoder(), shard_name_template='')
        )

        (features
            | GroupById()
            | Map(lambda (vessel_id, x): vessel_id)
            | beam.io.WriteToText(id_list_path, shard_name_template='')
        )

        return pipeline
