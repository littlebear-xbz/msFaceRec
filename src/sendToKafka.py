# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import os
import sys
from kafka import KafkaProducer
import time
import logging
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='./log/sendToKafka.log',
                    filemode='a'
                    )

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092', 'jp-bigdata-09:9092']
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=server_list)

# 定时发送测试数据 每隔十分钟发送两条数据
def sendByTime():
    count = 0
    while count < 10000:
        # line = "first+::::from windows::" + str(count)
        line1 = 'http://wh-dev:8009/Test/001.jpg' + \
                ":::" + str(datetime.datetime.now())
        line2 = 'http://139.219.102.23:8003/JojoAndPage.jpg' + \
                ":::" + str(datetime.datetime.now())
        producer.send(topic='mssend', value=line1)
        producer.send(topic='mssend', value=line2)
        logging.info("发送消息到kafka ，成功")
        logging.info(line1)
        logging.info(line2)
        producer.flush()
        time.sleep(600)
        count = count + 1

def send(url):
    producer.send(topic='mssend', value=url)
    logging.info("send by User:" + url)
    producer.flush()

if __name__ == "__main__" :
    if len(sys.argv) == 1 :
        logging.info("send by time, Every 10min send 2 messages")
        sendByTime()
    elif len(sys.argv) == 2:
        url = sys.argv[1]
        send(url)
