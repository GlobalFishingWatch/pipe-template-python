from __future__ import absolute_import
from apache_beam.options.pipeline_options import PipelineOptions

class CreateFeaturesOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument('--start_date', required=True,
                            help='Initial date' )
        required.add_argument('--end_date', required=True,
                            help='Final date')
        required.add_argument('--source_table', required=True,
                            help="BigQuery table where source data is located" )
        required.add_argument('--visits_table', required=True,
                            help="BigQuery table where source data is located" )
        required.add_argument('--sink_table', required=True,
                            help="BigQuery table where destination table is placed.")

        optional.add_argument('--sink_write_disposition',
                            help='How to merge the output of this process with whatever records are already' 
                                 'in the sink table. WRITE_TRUNCATE to remove all existing data and start fresh, or'\
                                 'WRITE_APPEND to add the new date without. Defaults to WRITE_APPEND.',
                            default='WRITE_APPEND')
        optional.add_argument('--wait',
                            help='Wait for Dataflow to finish before exiting',
                            action='store_true')
        optional.add_argument('--fast_test',
                            help='Run a small query for local testing',
                            action='store_true')

