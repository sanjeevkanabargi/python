import time
import os
import sys
import random

from google.cloud import pubsub_v1

project_id = "ba-qe-da7e1252"
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

def randomIP():
    arrVal = ["127.1.1.0","192.168.1.1","192.168.0.1","192.168.3.1","192.168.8.1","192.168.100.1","10.0.0.138"]
    return arrVal[random.randint(0,len(arrVal)-1)]


def main():  
   filepath = sys.argv[1]
   topic_name = sys.argv[2]
   count = 0

   while True :
      data = (randomIP()+","+randomIP()+","+randomIP()+","+randomIP()+","+randomIP()).encode('utf-8')
      message_future = publisher.publish(topic_path, data=data)
      message_future.add_done_callback(callback)
      count = count+1
      print(data)
      time.sleep(2)
      if count > 100 :
         exit()

  

if __name__ == '__main__':  
   main()