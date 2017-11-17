# -*- coding: utf-8 -*-
"""
Created on 2017/11/14
@author: xiongz
"""

from kafka import KafkaProducer
import time
import hdfs

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092', 'jp-bigdata-06:9092',
               'jp-bigdata-07:9092','jp-bigdata-08:9092', 'jp-bigdata-09:9092']

kafka_producer = KafkaProducer(bootstrap_servers=server_list)
HDFS_client = hdfs.InsecureClient("http://jp-bigdata-03:50070", user="hdfs")
with HDFS_client.read("/data/ods/jx_vehicle_data/20171112/a_20171107_vehicle_00.1510034074164.txt") as reader:
    for i in reader:
        print(i.strip())
        kafka_producer.send("ltest", value=i.strip())
        time.sleep(1)
