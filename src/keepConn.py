# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import sys
from kafka import KafkaProducer
import time
import ConfigParser
reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets
CF = ConfigParser.ConfigParser()
CF.read('../conf/conf.conf')
bootstrap_servers = CF.get("kafka","bootstrap").split(",")
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
kafka_topic = CF.get("kafka","topic_send")
while True:
    line = 'heart beat'
    producer.send(topic=kafka_topic, value=line)
    print line
    time.sleep(200)
    producer.flush()
