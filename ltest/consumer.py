# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 10:35:35 2017
@author: xiongz
"""
import sys
from kafka import KafkaConsumer
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import logging
import ConfigParser
CF = ConfigParser.ConfigParser()
CF.read('../conf/conf.conf')
reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='reply.log',
                    filemode='w'
                    )
# To consume latest messages and auto-commit offsets
server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092', 'jp-bigdata-09:9092']
# server_list = ['azure-mysql-01:9092']
server_list_jh = ['jh-hadoop-10:9092','jh-hadoop-11:9092','jh-hadoop-12:9092','jh-hadoop-13:9092','jh-hadoop-14:9092','jh-hadoop-15:9092',
                  'jh-hadoop-16:9092','jh-hadoop-17:9092','jh-hadoop-18:9092']
server_list_jx = ['jx-bigdata-03:9092', 'jx-bigdata-04:9092', 'jx-bigdata-05:9092','jx-bigdata-06:9092']

consumer = KafkaConsumer('ltest', group_id='groupJp',bootstrap_servers=server_list)


def listenTopic():
    count = 0
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        #  e.g., for unicode: `message.value.decode('utf-8')`
        yield message.value

def test():
    test = listenTopic()
    for i in test:
        print i

test()
