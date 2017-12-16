from apache_beam.options.pipeline_options import PipelineOptions

from pipe_tools.options import ReadFileAction

class TemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        optional.add_argument(
            '--tag_field',
            default='tag',
            help='name of field to add to generated messages. (default: %(default)s)')
        required.add_argument(
            '--tag_value',
            required=True,
            help='value to write to the tag field')
        required.add_argument(
            '--dest',
            required=True,
            help='local or gcs file to write output')
        optional.add_argument(
            '--wait',
            default=False,
            action='store_true',
            help='Wait until the job finishes before returning.')
