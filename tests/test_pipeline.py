import pytest

import pytest
import posixpath as pp
import newlinejson as nlj

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
# rename the class to prevent py.test from trying to collect TestPipeline as a unit test class

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards
from apache_beam import Map
from apache_beam import GroupByKey
from apache_beam import FlatMap

from pipe_tools.coders import JSONDictCoder
from pipe_tools.generator import MessageGenerator

import pipe_template
from pipe_template.__main__ import run as  pipe_template_run


@pytest.mark.filterwarnings('ignore:Using fallback coder:UserWarning')
@pytest.mark.filterwarnings('ignore:The compiler package is deprecated and removed in Python 3.x.:DeprecationWarning')
@pytest.mark.filterwarnings('ignore:open_shards is experimental.:FutureWarning')
class TestPipeline():

    def _run_pipeline (self, tag_field, tag_value, dest, expected, args=[]):
        args += [
            '--tag_field=%s' % tag_field,
            '--tag_value=%s' % tag_value,
            '--dest=%s' % dest,
            '--wait'
        ]

        pipe_template.__main__.run(args)

        with open_shards('%s*' % dest) as output:
            assert sorted(expected, key=lambda x: x['idx']) == sorted(nlj.load(output), key=lambda x: x['idx'])

    def test_Pipeline_basic_args(self, test_data_dir, temp_dir):
        dest = pp.join(temp_dir, 'messages.json')

        tag_value = 'test'
        expected = [dict(msg, tag=tag_value) for msg in MessageGenerator()]

        self._run_pipeline('tag', tag_value, dest, expected)

