#!/usr/bin/env python
import threading, logging, time
import multiprocessing
import sys

from kafka import KafkaConsumer, KafkaProducer

topic = "cfwTopic"
        
def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    filepath = str(sys.argv[1])

    with open(filepath) as fp:
        for line in fp:
            producer.send(topic, line.encode('utf-8'))
    
    producer.close()

        
        
if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()