from pipeline.shard_definition import PipelineDefinition
import apache_beam as beam
import logging
import pipeline.options.parser as parser

#TOOD: use Pauls options.
#TODO, two parsers.

def run():
    (options, pipeline_options) = parser.parse(shard_only=True)

    definition = PipelineDefinition(options)
    pipeline = definition.build(beam.Pipeline(options=pipeline_options))
    job = pipeline.run()

    if options.remote and options.wait:
        job.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()

