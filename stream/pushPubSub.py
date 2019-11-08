import time
import os
import sys

from google.cloud import pubsub_v1

#project_id = "data-qe-da7e1252"
project_id = "ba-qe-da7e1252"
#topic_name = "sk-firewall-pubsub"
topic_name = sys.argv[2]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())


def main():  
   filepath = sys.argv[1]
   topic_name = sys.argv[2]
   if not os.path.isfile(filepath):
      print("File path {} does not exist. Exiting...".format(filepath))
      sys.exit()
   count = 0
   with open(filepath) as fp:
      for line in fp:
         data = line.encode('utf-8')
         # When you publish a message, the client returns a Future.
         message_future = publisher.publish(topic_path, data=data)
         message_future.add_done_callback(callback)
         count = count+1
         time.sleep(10)
         if count > 40 :
            exit()
            

if __name__ == '__main__':  
   main()