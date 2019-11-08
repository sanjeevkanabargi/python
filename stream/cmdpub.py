import sys  
import os
import time
from google.cloud import pubsub_v1


project_id = "data-qe-da7e1252"
topic_name = "projects/data-qe-da7e1252/topics/sk-firewall-json"
publisher = pubsub_v1.PublisherClient()

def publish_messages(line):
  """Publishes multiple messages to a Pub/Sub topic.""" 
  command = "gcloud beta pubsub topics publish "+ topic_name+" --message "+'"'+str(line)+'"'
  os.system(command)




def main():  
  filepath = sys.argv[1]

  if not os.path.isfile(filepath):
    print("File path {} does not exist. Exiting...".format(filepath))
    sys.exit()
  count = 0
  with open(filepath) as fp:

    for line in fp:
      print(str(count)+" : line : "+line)
      publish_messages(line)
      #time.sleep(2)
      count += 1
      if count > 10 :
        exit()
  print("This is done.")

if __name__ == '__main__':  
   main()