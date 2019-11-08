import json
import random

filepath = "/Users/skanabargi/dataSource/firewall/cfw-less-data"


def generateDict(field):
    randDic = {field : {
		"AccuracyRadius": random.randint(1,101),
		"Latitude": random.randint(1,101),
		"Longitude": random.randint(1,101),
		"MetroCode": random.randint(1,101),
		"TimeZone": "IST"
	}}

    return randDic

#Read json from big file.
with open(filepath) as fp:
      for line in fp:
          #print(line)
          jsonString = json.loads(line)
          print(type(jsonString))
          #jsonString.
          dest = generateDict("dst_ip_location")
          src = generateDict("src_ip_location")

          jsonString.update(dest)
          jsonString.update(src)

          print(jsonString)

#Change into extended json message with extra data. 