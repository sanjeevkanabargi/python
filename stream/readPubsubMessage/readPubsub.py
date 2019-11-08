import argparse
import os

"""Receives messages from a pull subscription."""

import datetime, time
from google.cloud import pubsub_v1


parser = argparse.ArgumentParser()
parser.add_argument('-p','--projectID', help="Project ID fo Gcloud <ba-qe-da7e1252>", default='ba-qe-da7e1252')
parser.add_argument('-s','--subscriber', help="Subscriber of a pubsub topic", required=True)
parser.add_argument('-k','--serviceKey', help="GCP service account key, json file path", required=True)

args = parser.parse_args()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(args.serviceKey)
project_id = args.projectID
subscription_name =  args.subscriber

print('Pulling data form : '+str(project_id)+"/"+str(subscription_name))

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_name}`
subscription_path = subscriber.subscription_path(
    project_id, subscription_name)

def callback(message):
    print('======================================\nReceived msge:\t{} \nReceived time:\t{}\n'.format(message.data.decode("utf-8"), datetime.datetime.now()))
    #message.ack()

subscriber.subscribe(subscription_path, callback=callback)

# The subscriber is non-blocking. We must keep the main thread from
# exiting to allow it to process messages asynchronously in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    i=1
# [END pubsub_subscriber_async_pull]
# [END pubsub_quickstart_subscriber]


