from argparse import ArgumentParser
import pipeline.options.all as all
import pipeline.options.local as local
import pipeline.options.remote as remote
import pipeline.options.google as google
import apache_beam.options.pipeline_options as beam

# We need a custom options class to serialize and store additional options we
# are parsing. We are not really using these options in our code, but when
# running on dataflow this makes these options available on the dataflow
# console.
class CustomOptions(beam.PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        all.setup(parser)
        remote.setup(parser)

def setup_global_options(parser):
    all.setup(parser)

def setup_local_command(commands):
    local_command = commands.add_parser(
        'local',
        help='Run the pipelines locally',
        description='Run a local pipeline which outputs results to local json files.',
    )
    local_command.set_defaults(local=True, remote=False)
    local.setup(local_command)

def setup_remote_command(commands):
    remote_command = commands.add_parser(
        'remote',
        help='Run the pipeline in google cloud dataflow',
        description='Run the pipeline in google cloud dataflow.',
    )
    remote_command.set_defaults(remote=True, local=False)
    remote.setup(remote_command)
    google.setup(remote_command)

def parse():
    parser = ArgumentParser(prog="pipeline")
    setup_global_options(parser)

    commands = parser.add_subparsers(title="subcommands")
    setup_local_command(commands)
    setup_remote_command(commands)

    options = parser.parse_args()

    # Set option values for beam pipeline options based on our parsed options
    pipeline_options = beam.PipelineOptions()
    if options.local:
        standard_options = pipeline_options.view_as(beam.StandardOptions)
        standard_options.runner = 'DirectRunner'
    elif options.remote:
        standard_options = pipeline_options.view_as(beam.StandardOptions)
        standard_options.runner = 'DataflowRunner'
        setup_options = pipeline_options.view_as(beam.SetupOptions)
        setup_options.setup_file = './setup.py'

    return (options, pipeline_options)
