# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import os
import sys
from kafka import KafkaProducer
import time

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092', 'jp-bigdata-06:9092', 'jp-bigdata-07:9092',
               'jp-bigdata-08:9092', 'jp-bigdata-09:9092']
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=server_list)

count = 0
while count < 2:
    # line = "first+::::from windows::" + str(count)
    line = """http://139.219.102.23:8003/JojoAndPage.jpg,success,http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485,http://wh-dev:8009/JojoAndPage/JojoAndPage_2/JojoAndPage_2.jpg|C:\zhiqian\Faces\PDB\436\06-snap0583.jpg|06-snap0583|0.6539416"""
    line2 = "url,success,list1,list2,list3"
    line3 = "http://139.219.102.23:8003/JojoAndPage.jpg"
    line4 = "aaa"
    line5 = "http://jp-bigdata-01/images/ltest7.jpg"
    producer.send(topic='mssend', value=line4)
    # producer.send(topic='msreply', value=line5)
    print line4
    time.sleep(2)
    count = count+1
producer.flush()
