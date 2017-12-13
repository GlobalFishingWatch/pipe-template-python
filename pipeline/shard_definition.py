import os
from pipeline.transforms.source import Source
from pipeline.transforms.byte_encode import ByteEncode
from pipeline.transforms.group_by_id import GroupById
from pipeline.objects.location_record import LocationRecordsFromDicts
from pipeline.objects.feature import Feature
from pipeline.objects.feature import FeaturesToDicts
from pipeline.objects import namedtuples
from pipeline.schemas.output import build as build_output_schema
from pipeline.utils.keypartitionedtfsink import WriteToKeyPartitionedTFFiles
from pipe_tools.coders.jsoncoder import JSONDict
import apache_beam as beam
from apache_beam import io
from apache_beam.pvalue import AsList
from apache_beam import Flatten
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam.transforms.window import TimestampedValue

import datetime
import tensorflow as tf


class ByteCoder(beam.coders.Coder):
    """A coder used for writing features"""

    def encode(self, value):
        return value['value']

    def decode(self, value):
        raise NotImplementedError

    def is_deterministic(self):
        return True



class ExtractIdsFn(beam.CombineFn):

  def create_accumulator(self):
    return set()

  def add_input(self, ids, x):
    ids.add(x.id)
    return ids

  def merge_accumulators(self, accumulators):
    accumulators = list(accumulators)
    ids = accumulators[0]
    for x in accumulators[1:]:
        ids |= x
    return ids

  def extract_output(self, ids):
    return sorted(ids)


def build_example(item):
    vessel_id, features_seq = item
    example = tf.train.SequenceExample()
    example.context.feature["mmsi"].int64_list.value.append(int(vessel_id))
    for values in sorted(features_seq, key = lambda x: x.timestamp):
        assert values[0] == vessel_id
        movement_features = example.feature_lists.feature_list["movement_features"]
        feats = [namedtuples._datetime_to_s(values[1])] + list(values[2:])
        movement_features.feature.add().float_list.value.extend(feats)
    return JSONDict(id=vessel_id, value=example.SerializeToString())


class PipelineDefinition():

    def __init__(self, options):
        self.options = options

    def build(self, pipeline):

        # WriteToIdPartitionedFiles mangles the given path, using the last item as the filename,
        # and inserting a directory named after the id before the path
        shard_pseudo_path = os.path.join(self.options.shard_location, 'features', '')

        # id_list_path = os.path.join(self.options.shard_location, 'idlist.txt')
        id_list_path = os.path.join(self.options.shard_location, 'mmsis/part-00000-of-00001.txt')

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
            | GroupById()
            | Map(build_example)
            | WriteToKeyPartitionedTFFiles('id', shard_pseudo_path, coder=ByteCoder(), 
                                            shard_name_template='', file_name_suffix='.tfrecord')
        )

        # TODO: just do this as another query, or using generalized reduction. Don't need 
        # groupby
        (features
            | beam.CombineGlobally(ExtractIdsFn())
            | beam.FlatMap(lambda x: x)
            | beam.io.WriteToText(id_list_path, shard_name_template='')
        )

        return pipeline
