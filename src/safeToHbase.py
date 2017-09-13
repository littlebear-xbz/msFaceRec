# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:28:25 2017
@author: Xiongz
"""

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
import datetime
import logging
from hbase.ttypes import *
from kafka import KafkaConsumer
import sys
reload(sys)
sys.setdefaultencoding('utf8')

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='./log/sendToKafka.log',
                    filemode='a'
                    )

Hbase_url = "jp-bigdata-03"
Hbase_port = 9090
transport = TTransport.TBufferedTransport(TSocket.TSocket(Hbase_url, Hbase_port))
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
table_name = 'tab_msFaceRec_recived'

def safeToHbase():
    pass