import random
import json
from datetime import datetime, timedelta
import csv


timestamp = datetime.now() + timedelta(days=-3)

def randomIP():
    arrVal = ["127.1.1.0","192.168.1.1","192.168.0.1","192.168.3.1","192.168.8.1","192.168.100.1","10.0.0.138"]
    return arrVal[random.randint(0,len(arrVal)-1)]


def randomPort():
    return random.randint(1000,1050)


def randomSession():
    return random.randint(2000,2050)


def randomValue(arrVal):
    #print("returning : "+arrVal[random.randint(0,len(arrVal)-1)])
    return arrVal[random.randint(0,len(arrVal)-1)]


def getRandomIPLoc():
    return {
		"AccuracyRadius": random.randint(1,101),
		"Latitude": random.randint(1,101),
		"Longitude": random.randint(1,101),
		"MetroCode": random.randint(1,101),
		"TimeZone": "IST"
	}

def createRandomData():
    data = {}

    timestamp = getFromatedDate(gettimestamp())
    #print("thje date :"+str(timestamp))
    data['timestamp'] = timestamp
    data['event_type'] = randomValue(["flow-event","login","logout","download","upload"])
    data['action'] = randomValue(["allow","deny","auth","start","stope"])
    data['src_ip'] = randomIP()
    data['dst_ip'] = randomIP()
    data['nat_src_ip'] = randomIP()
    data['nat_src_port'] = randomPort()
    data['session_id'] = randomSession()
    data['src_port'] = randomPort()
    data['dst_port'] = randomPort()
    data['protocol'] = randomValue(["http","https","ftp"])
    appid = randomValue(["facebook","linkedin","google","netskope","zomato","swiggy"])
    data['app_id']= appid
    data['rule_name'] = str(appid)+"_rule"
    data['user_name'] = randomValue(["Sanjeev","Ying","Muneer","Azham","Aniket","Linto","Tejasvi"])
    data['repeat_count'] = random.randint(0,10)
    data['bytes_sent'] = random.randint(100,500)
    data['bytes_rcvd'] = random.randint(100,500)
    data['packet_sent'] = random.randint(10,50)
    data['packet_rcvd'] = random.randint(10,50)
    data['start_time'] = timestamp
    data['session_duration'] = random.randint(1,50)
    data['tunnel_type'] = randomValue(["ipsec","L2TP","PPTP","TLS","ssh","OpenVPN"])
    data['tenant_id'] = randomValue(["VM","Wallmart","Nike","Addidas","HP","Cisco","Google","Flipkart"])
    #data['dst_ip_location'] = getRandomIPLoc()
    #data['src_ip_location'] = getRandomIPLoc()

    return data

    #Create JSON message from above variables. 

    #create CSV files from above message. 


def gettimestamp():
    return timestamp - timedelta(minutes=-1)

def getFromatedDate(value):
    mylist = []
    mylist.append(value)
    tempVal = str(mylist[0]).split(" ")
    toPrint = tempVal[0]+"T"+tempVal[1].split(".")[0]
    #print(toPrint)
    return toPrint

if __name__ == '__main__':
    #message = createRandomData()
    #print(message)
    #get Timestamps.. 
    timestamp = datetime.now() + timedelta(days=-3)
    #src_port,dst_port,protocol,app_id,rule_name,user_name,repeat_count,bytes_sent,bytes_rcvd,packet_sent,packet_rcvd,start_time,session_duration,tunnel_type,tenant_id,dst_ip_location.AccuracyRadius,dst_ip_location.Latitude,dst_ip_location.Longitude,dst_ip_location.MetroCode,dst_ip_location.TimeZone,src_ip_location.AccuracyRadius,src_ip_location.Latitude,src_ip_location.Longitude,src_ip_location.MetroCode,src_ip_location.TimeZone,
    #data = 

    with open('cfw-sample.json', 'w') as f:  # Just use 'w' mode in 3.x
        for i in range(10000000):
            data = createRandomData()
            jsondata = json.dumps(data)
            f.write(jsondata+"\n")