#parsing json 


import sys  
import os
import time
import json





def main():  
   filepath = sys.argv[1]

   if not os.path.isfile(filepath):
       print("File path {} does not exist. Exiting...".format(filepath))
       sys.exit()
   count = 0
   with open(filepath) as fp:
		for line in fp:
			csvRecord = ""
			firstElement = True
			#print line+"\n"
			count = count +1
			data = json.loads(line)
			for column in data:
				if firstElement :
					csvRecord = str(data[column])
					firstElement = False
				else :
					csvRecord = csvRecord+","+str(data[column])
			print "CSV Records : "+csvRecord
			if count > 5 :
				exit()

if __name__ == '__main__':  
   main()