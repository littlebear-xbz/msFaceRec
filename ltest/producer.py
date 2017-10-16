# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import os
import sys
from kafka import KafkaProducer
import time
import random

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092', 'jp-bigdata-06:9092']
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=server_list)

lines = [
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/ltest1.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/ltest2.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/ltest3.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/ltest4.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01/images/cannotfindpeople.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01/images/cannotfindpeople1.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01/images/fail.jpg"}"""
]

count = 0
while count < 1000:
    line = random.choice(lines)
    producer.send(topic='mssend', value=line)
    print line
    time.sleep(400)
    count = count+1
# producer.flush()
