import tensorflow as tf
from apache_beam.coders import BytesCoder
from pipe_tools.io.keypartitionedsink import KeyPartitionedFileSink
from pipe_tools.io.keypartitionedsink import WriteToKeyPartitionedFiles
from pipe_tools.io.keypartitionedsink import KeyShardDoFn
from pipe_tools.io.keypartitionedsink import DEFAULT_SHARDS_PER_KEY
from pipe_tools.io.keypartitionedsink import CompressionTypes



class WriteToKeyPartitionedTFFiles(WriteToKeyPartitionedFiles):
    """
    Write the incoming pcoll to files partitioned by a string key.  The key is taken from the
    a field in each element specified by 'key'.

    """
    def __init__(self,
                 key_field,
                 file_path_prefix,
                 file_name_suffix='',
                 shards_per_key=DEFAULT_SHARDS_PER_KEY,
                 shard_name_template=None,
                 coder=BytesCoder()):

        self.shards_per_key = shards_per_key or DEFAULT_SHARDS_PER_KEY

        self._sink = KeyPartitionedTFSink(file_path_prefix,
                                             file_name_suffix=file_name_suffix,
                                             shard_name_template=shard_name_template,
                                             coder=coder)

        self._sharder = KeyShardDoFn(key_field, shards_per_key=self.shards_per_key)



class KeyPartitionedTFSink(KeyPartitionedFileSink):

    def __init__(self,
                 file_path_prefix,
                 file_name_suffix='',
                 shard_name_template=None,
                 coder=BytesCoder()):

        super(KeyPartitionedTFSink, self).__init__(
                 file_path_prefix,
                 file_name_suffix,
                 append_trailing_newlines=False,
                 shard_name_template=shard_name_template,
                 coder=coder,
                 # TODO: There is compression that can be set, implement
                 compression_type=CompressionTypes.AUTO, 
                 header=None)

    def open(self, temp_path):
        return tf.python_io.TFRecordWriter(temp_path)


