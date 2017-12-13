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
        # return str(value['timestamp'])

    def decode(self, value):
        raise NotImplementedError

    def is_deterministic(self):
        return True


def _int64_feature(value):
  return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

def _floats_feature(values):
  return tf.train.Feature(float_list=tf.train.FloatList(value=values))


# class ProtbufCoder(beam.coders.Coder):
#     """A coder used for writing features"""

#     def encode(self, x):
#         x['values'] = list(x['values'])
#         x['values'][0] = namedtuples._datetime_to_s(x['values'][0])
        
#         # See https://github.com/tensorflow/tensorflow/blob/r1.4/tensorflow/core/example/feature.proto
#         # https://github.com/tensorflow/tensorflow/blob/r1.4/tensorflow/examples/how_tos/reading_data/convert_to_records.py
#         # example = tf.train.Example(features=tf.train.Features(feature={
#         #     'mmsi': _int64_feature(int(x['id'])),
#         #     'movement_features' : _floats_feature(x['values'])
#         #     }))
#         example = tf.train.SequenceExample()
#         example.context.feature["mmsi"].int64_list.value.append(sequence_length)

#         movement_features = example.feature_lists.feature_list["movement_features"]

#         for values in x['values']:
#             movement_features.features.add().feature_list.value.append(
#                 tf.train.Feature(float_list=tf.train.FloatList(value=vals))
#                 )

#     # 3D length
#     sequence_length = len(input_sequence)


#     input_characters = example_sequence.feature_lists.feature_list["input_characters"]
#     output_characters = example_sequence.feature_lists.feature_list["output_characters"]

#     for input_character, output_character in zip(input_sequence,
#                                                           output_sequence):

#         if input_sequence is not None:
#             input_characters.feature.add().int64_list.value.append(input_character)

#         if output_characters is not None:
#             output_characters.feature.add().int64_list.value.append(output_character)



#         example = tf.train.Example(features=tf.train.Features(feature={
#             'mmsi': _int64_feature(int(x['id'])),
#             'movement_features' : _floats_feature(x['values'])
#             }))
#         return example.SerializeToString()

#     def decode(self, value):
#         raise NotImplementedError

#     def is_deterministic(self):
#         return True


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


def build_examples(item):
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
            | Map(build_examples)
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
