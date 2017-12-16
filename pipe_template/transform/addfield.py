import ujson
import six

import apache_beam as beam

from pipe_tools.coders import JSONDictCoder
from pipe_tools.coders import JSONDict
from pipe_tools.timestamp import TimestampedValueDoFn
from pipe_tools.io import WriteToDatePartitionedFiles


class AddField(beam.DoFn):
    def __init__(self, field, value):
        self.field = field
        self.value = value


    def process(self, element):
        element[self.field] = self.value
        yield element
