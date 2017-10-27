# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import os
import sys
from kafka import KafkaProducer
import time
import json
import random

reload(sys)
sys.setdefaultencoding('utf-8')
# To consume latest messages and auto-commit offsets

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092', 'jp-bigdata-06:9092']
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=server_list)

lines = [
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/1.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/2.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/3.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/4.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/5.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/6.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/7.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/8.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/9.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/10.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/11.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/12.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/13.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/15.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/16.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/17.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/18.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/19.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/20.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/21.jpg"}""",
"""{"ImgNum":"12","CameraId":12,"CameraIp":"12","PassTime":"12","FaceUrl":"http://jp-bigdata-01:/images/22/full/22.jpg"}"""
]

count = 0
while count < 2:
    line = lines[count]
    producer.send(topic='mssend', value=line)
    print line
    time.sleep(0.1)
    count = count+1
# producer.flush()
