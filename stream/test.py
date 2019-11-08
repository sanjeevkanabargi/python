import sys  
import os
import time
from google.cloud import pubsub_v1


def main():  
   filepath = sys.argv[1]

   if not os.path.isfile(filepath):
       print("File path {} does not exist. Exiting...".format(filepath))
       sys.exit()
   count = 0
   with open(filepath) as fp:

       for line in fp:
           #new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"),
           strArr = line.split("\t");
           #print strArr;
           data = 'new TableFieldSchema().setName("'+strArr[0]+'").setType("'+strArr[1]+'").setMode("'+strArr[2]+'"),'
           
           #.name("name").type().stringType().noDefault()
           #data = '.name("'+strArr[0]+'").type().'+strArr[1]+'().noDefault()'

           print(data);

if __name__ == '__main__':  
   main()