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



def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""


  pubsubTopicName = "projects/data-qe-da7e1252/topics/sk-firewall-json"

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      # CHANGE 1/5: The Google Cloud Storage path is required
                      # for outputting the results.
                      #default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
                      #default="/Users/skanabargi/python/stream/output",
                      default='gs://data-qe-da7e1252/tmp/sk_out',
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_args.extend([
      # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DataflowRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project=data-qe-da7e1252',
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      #'--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
      '--staging_location=gs://data-qe-da7e1252/tmp/stage/',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      #'--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
      '--temp_location=gs://data-qe-da7e1252/tmp/local',
      '--experiments=allow_non_updatable_job',
      '--job_name=sk-pubsub-to-gcs-5',
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
    output = ( lines | 'window' >> beam.WindowInto(window.FixedWindows(60)))

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'writeTOGcs' >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()