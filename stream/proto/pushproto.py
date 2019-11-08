import time
import os
import sys
import json
import random

from google.cloud import pubsub_v1

from google.protobuf import json_format
import cfw_pb2

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

def generateDict(field):
    randDic = {field : {
		"AccuracyRadius": random.randint(1,101),
		"Latitude": random.randint(1,101),
		"Longitude": random.randint(1,101),
		"MetroCode": random.randint(1,101),
		"TimeZone": "IST"
	}}

    return randDic


def main():  
   filepath = sys.argv[1]
   topic_name = sys.argv[2]
   if not os.path.isfile(filepath):
      print("File path {} does not exist. Exiting...".format(filepath))
      sys.exit()
   count = 0
   with open(filepath) as fp:
      for line in fp:
         jsonString = json.loads(line)
         #print(type(jsonString))
         #jsonString.
         dest = generateDict("dst_ip_location")
         src = generateDict("src_ip_location")

         jsonString.update(dest)
         jsonString.update(src)

         jsonmsg = '{"timestamp": "2019-05-21T14:24", "event_type": "flow-event", "action": "allow", "src_ip": "1.1.1.1", "dst_ip": "10.1.1.1", "nat_src_ip": "172.168.32.23", "nat_src_port": 100, "session_id": 45454, "src_port": 104, "dst_port": 4063, "protocol": "http", "app_id": "facebook", "rule_name": "facebook_rule", "user_name": "aniket", "repeat_count": 9, "bytes_sent": 3836, "bytes_rcvd": 4625, "packet_sent": 32, "packet_rcvd": 6, "start_time": "2019-05-21T14:24", "session_duration": 41, "tunnel_type": "ipsec", "tenant_id": "vw", "dst_ip_location": {"AccuracyRadius": 90, "Latitude": 17, "Longitude": 97, "MetroCode": 83, "TimeZone": "IST"}, "src_ip_location": {"AccuracyRadius": 13, "Latitude": 44, "Longitude": 86, "MetroCode": 11, "TimeZone": "IST"}}'
         
         rawbytes = json_format.Parse(jsonmsg, cfw_pb2.cfe(), ignore_unknown_fields=False)

         print(rawbytes)
         #data = str(jsonString).encode('utf-8')
         # When you publish a message, the client returns a Future.
         message_future = publisher.publish(topic_path, data=rawbytes.SerializeToString())
         message_future.add_done_callback(callback)
         count = count+1
         #time.sleep(4)
         if count > 0 :
            exit()

if __name__ == '__main__':  
   main()