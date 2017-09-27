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
import random
reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='../log/sendToKafka.log',
                    filemode='w'
                    )

# server_list = ['jh-hadoop-10:9092','jh-hadoop-11:9092','jh-hadoop-12:9092','jh-hadoop-13:9092','jh-hadoop-14:9092','jh-hadoop-15:9092',
#                   'jh-hadoop-16:9092','jh-hadoop-17:9092','jh-hadoop-18:9092',]

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092', 'jp-bigdata-06:9092', 'jp-bigdata-07:9092',
               'jp-bigdata-08:9092', 'jp-bigdata-09:9092']
# server_list = ['azure-mysql-01:9092']
producer = KafkaProducer(bootstrap_servers=server_list)
"""
url_list = ['http://jh-hadoop-02/images/ltest1.jpg',
            'http://jh-hadoop-02/images/ltest2.jpg',
            'http://jh-hadoop-02/images/ltest3.jpg',
            'http://jh-hadoop-02/images/ltest4.jpg',
            'http://jh-hadoop-02/images/ltest5.jpg',
            'http://jh-hadoop-02/images/ltest6.jpg',
            'http://jh-hadoop-02/images/ltest7.jpg',
            'http://jh-hadoop-02/images/ltest8.jpg',
            'http://jh-hadoop-02/images/ltest9.jpg',
            'http://jh-hadoop-02/images/ltest10.jpg',
            'Bad Url',
            'http://jh-hadoop-02/images/ltest11.jpg',
            'http://jh-hadoop-02/images/ltest12.jpg'
            'http://jh-hadoop-02/images/ltest13.jpg']
"""
url_list = ['http://jp-bigdata-01/images/ltest1.jpg',
            'http://jp-bigdata-01/images/ltest2.jpg',
            'http://jp-bigdata-01/images/ltest3.jpg',
            'http://jp-bigdata-01/images/ltest4.jpg',
            'http://jp-bigdata-01/images/ltest5.jpg',
            'http://jp-bigdata-01/images/ltest6.jpg',
            'http://jp-bigdata-01/images/ltest7.jpg',
            'http://jp-bigdata-01/images/ltest8.jpg',
            'http://jp-bigdata-01/images/ltest9.jpg',
            'http://jp-bigdata-01/images/ltest10.jpg',
            'http://jp-bigdata-01/images/cannotfindpeople.jpg',
            'http://jp-bigdata-01/images/nopeople.jpg']
# 定时发送测试数据 每隔十分钟发送两条数据
def sendByTime():
    count = 0
    while count < 10000:
        # line = "first+::::from windows::" + str(count)
        line = random.choice(url_list)
        producer.send(topic='mssend', value=line)
        logging.info("Send to kafka ，success" + line)
        producer.flush()
        # time.sleep(600)
        count = count + 1
        print count

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


