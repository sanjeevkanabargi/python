from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam import window

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.combiners import ToListCombineFn


TABLE_SCHEMA = ('name:STRING, empid:STRING, '
                'pin:INTEGER, salary:FLOAT')


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""


  pubsubTopicName = "projects/data-qe-da7e1252/topics/sk-firewall-json"
  bigqueryTableID = "data-qe-da7e1252:dataflow_to_bigquery.emp"

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      default='gs://data-qe-da7e1252/tmp/sk_out',
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_args.extend([
      '--runner=DataflowRunner',
      '--project=data-qe-da7e1252',
      '--staging_location=gs://data-qe-da7e1252/tmp/stage/',
      '--temp_location=gs://data-qe-da7e1252/tmp/local',
      '--experiments=allow_non_updatable_job',
      '--job_name=sk-pubsub-to-gcs-8',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    #lines = p | ReadFromText(known_args.input)
    lines = p | beam.io.ReadFromPubSub(topic=pubsubTopicName)

    # Count the occurrences of each word.
    output = ( lines 
#       | 'Window' >> beam.WindowInto(window.FixedWindows(60))
 #      | 'Collect' >> beam.CombineGlobally(ToListCombineFn()).without_defaults()
       | 'Parse' >> beam.ParDo(FormDoFn()))

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    #output | 'Write to BQ' >> WriteToText(known_args.output)

    #writing into bigquery
    outputTable = "data-qe-da7e1252:dataflow_to_bigquery.emp"
    output | 'Write to BQ' >> beam.io.WriteToBigQuery(
        outputTable,
        schema=TABLE_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

class FormDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        arrData = element;
        return [{'name':arrData[0],'empid':arrData[1],'pin':arrData[2],'salary':arrData[3]}]


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()