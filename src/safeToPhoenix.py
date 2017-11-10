# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:28:25 2017
@author: Xiongz
"""
from kafka import KafkaConsumer
import phoenixdb
import logging
import hashlib

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='../log/safeToPhoenix.log',
                    filemode='w'
                    )

database_url = 'http://jp-bigdata-01:8765/'
# database_url = 'http://jh-hadoop-02:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
cursor = conn.cursor()
cnx = conn

server_list = ['jp-bigdata-03:9092', 'jp-bigdata-04:9092', 'jp-bigdata-05:9092',
               'jp-bigdata-06:9092', 'jp-bigdata-07:9092', 'jp-bigdata-08:9092', 'jp-bigdata-09:9092']

# server_list = ['jh-hadoop-10:9092','jh-hadoop-11:9092','jh-hadoop-12:9092','jh-hadoop-13:9092','jh-hadoop-14:9092','jh-hadoop-15:9092',
#                   'jh-hadoop-16:9092','jh-hadoop-17:9092','jh-hadoop-18:9092',]
consumer = KafkaConsumer('msreply', group_id='groupltest',bootstrap_servers=server_list)

print "start"
for message in consumer:
    recived_message = message.value
    messagelist = recived_message.split(",")
    logging.info(messagelist)
    print messagelist
    if len(messagelist) == 3:
        if messagelist[1] == 'fail':
            logging.info(messagelist[0] + "---url not Rec")
        elif messagelist[1] == 'noAvatar':
            logging.info(messagelist[0] + '---not found face')
        elif messagelist[1] == 'noCard':
            logging.info(messagelist[0] + '---not VIP')
        recived_url_send = messagelist[0]
        recived_time = messagelist[2]
        recived_status = messagelist[1]
        # print str(messagelist) + "fail"
        rowkey = hashlib.md5(recived_url_send + recived_time).hexdigest() + "|" + recived_time \
                 + "|" + recived_url_send
        sql_phoenix = """UPSERT INTO ods.ODS_MSFACEREC_RECIVED(RowSets,send_url,recived_time,status)
                        VALUES('%(rowkey)s',
                        '%(recived_url_send)s',
                        '%(date)s',
                        '%(status)s'
                        )
                        """ % {"rowkey": rowkey, 'recived_url_send': recived_url_send, "date": recived_time,
                               "status": recived_status}
        logging.info("safe To Phoenix:" + sql_phoenix)
        cursor.execute(sql_phoenix)
    elif len(messagelist) < 3 or len(messagelist) > 12:
        logging.error("message is error")
        logging.error(recived_message)
    elif len(messagelist) > 3 and len(messagelist) <= 13:
        recived_url_send = messagelist[0]
        recived_status = messagelist[1]
        recived_time = messagelist[2]
        recived_results = ['', '', '', '', '', '', '', '', '', '']
        rowkey = hashlib.md5(recived_url_send + recived_time).hexdigest() + "|" + recived_time \
                 + "|" + recived_url_send
        logging.debug(rowkey)
        logging.debug("list lenth:::" + str(len(messagelist)))
        for i in range(3, len(messagelist)):
            recived_results[i - 3] = messagelist[i]
        # print "recived_results "
        # print recived_results
        sql_phoenix = """UPSERT INTO ods.ODS_MSFACEREC_RECIVED(RowSets,send_url,recived_time,status,result_1,result_2,result_3
                ,result_4,result_5,result_6,result_7,result_8,result_9,result_10)
                VALUES('%(rowkey)s',
                '%(recived_url_send)s',
                '%(date)s',
                '%(status)s',
                '%(result_1)s','%(result_2)s','%(result_3)s','%(result_4)s','%(result_5)s','%(result_6)s',
                '%(result_7)s','%(result_8)s','%(result_9)s','%(result_10)s'
                )
                """ % {"rowkey": rowkey, 'recived_url_send': recived_url_send, "date": recived_time,
                       "status": recived_status,
                       "result_1": recived_results[0], "result_2": recived_results[1], "result_3": recived_results[2],
                       "result_4": recived_results[3], "result_5": recived_results[4], "result_6": recived_results[5],
                       "result_7": recived_results[6], "result_8": recived_results[7], "result_9": recived_results[8],
                       "result_10": recived_results[9]}
        logging.info("safe To Phoenix:" + sql_phoenix)
        cursor.execute(sql_phoenix)


