import time

from google.cloud import pubsub_v1
import datetime


# TODO project_id = "Your Google Cloud Project ID"
# TODO subscription_name = "Your Pub/Sub subscription name"

project_id = "ba-qe-da7e1252"
subscription_name = "sk_today_sub1"

a = datetime.date.today()
b = a + datetime.timedelta(seconds=10)
count = 0


print(a)
print(b)
subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_name}`
subscription_path = subscriber.subscription_path(
    project_id, subscription_name)



def callback(message):
    print(str(message.data))
    print("\n")
    message.ack()

subscriber.subscribe(subscription_path, callback=callback)

# The subscriber is non-blocking. We must keep the main thread from
# exiting to allow it to process messages asynchronously in the background.
print('Listening for messages on {}'.format(subscription_path))
while True:
    time.sleep(60)