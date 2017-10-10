# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:28:25 2017
@author: Xiongz

保存接受数据到数据库，一份保存到phoenix，一份保存到mysql
"""

from kafka import KafkaConsumer
import phoenixdb
import logging
import hashlib
import datetime
import ConfigParser
import pymysql
CF = ConfigParser.ConfigParser()
CF.read('../conf/conf.conf')

logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='../log/safeToDb.log',
                    filemode='w'
                    )

def safeToPhoenix(data,cursor):
    recived_message = data
    messagelist = recived_message.split(",")
    logging.info(messagelist)
    print messagelist
    if len(messagelist) == 2:
        if messagelist[1] == 'fail':
            logging.warning(messagelist[0] + "---url not Rec")
        elif messagelist[1] == 'noAvatar':
            logging.warning(messagelist[0] + '---not found face')
        elif messagelist[1] == 'noCard':
            logging.warning(messagelist[0] + '---not VIP')
        recived_url_send = messagelist[0]
        recived_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        recived_status = messagelist[1]
        print str(messagelist) + "fail"
        rowkey = hashlib.md5(recived_url_send).hexdigest() + datetime.datetime.now().strftime('%Y%m%d%H%M%S') \
                 + recived_url_send
        sql_phoenix = """UPSERT INTO ods.ODS_MSFACEREC_RECIVED(RowSets,send_url,recived_time,status)
                    VALUES('%(rowkey)s',
                    '%(recived_url_send)s',
                    '%(date)s',
                    '%(status)s'
                    )
                    """ % {"rowkey": rowkey, 'recived_url_send': recived_url_send, "date": recived_time,
                           "status": recived_status}
        logging.info(sql_phoenix)
        cursor.execute(sql_phoenix)
    elif len(messagelist) < 2 or len(messagelist) > 12:
        logging.error("message is error")
        logging.error(recived_message)
    elif len(messagelist) > 2 and len(messagelist) <= 12:
        recived_url_send = messagelist[0]
        recived_status = messagelist[1]
        recived_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        recived_results = ['', '', '', '', '', '', '', '', '', '']
        rowkey = hashlib.md5(recived_url_send).hexdigest() + datetime.datetime.now().strftime('%Y%m%d%H%M%S') \
                 + recived_url_send
        logging.info(rowkey)
        logging.info("list lenth:::" + str(len(messagelist)))
        for i in range(2, len(messagelist)):
            recived_results[i - 2] = messagelist[i]
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
        logging.info(sql_phoenix)
        cursor.execute(sql_phoenix)

def safeToMysql(data,cursor,conn):
    recived_message = data
    messagelist = recived_message.split(",")
    logging.info("mysql data" + str(messagelist))
    print messagelist
    if len(messagelist) == 2:
        if messagelist[1] == 'fail':
            logging.warning(messagelist[0] + "---url not Rec")
        elif messagelist[1] == 'noAvatar':
            logging.warning(messagelist[0] + '---not found face')
        elif messagelist[1] == 'noCard':
            logging.warning(messagelist[0] + '---not VIP')
        recived_url_send = messagelist[0]
        recived_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        recived_status = messagelist[1]
        print str(messagelist) + "fail"
        rowkey = hashlib.md5(recived_url_send).hexdigest() + datetime.datetime.now().strftime('%Y%m%d%H%M%S') \
                 + recived_url_send
        sql_mysql = """INSERT INTO ODS_MSFACEREC_RECIVED(RowSets,send_url,recived_time,status)
                        VALUES('%(rowkey)s',
                        '%(recived_url_send)s',
                        '%(date)s',
                        '%(status)s'
                        )
                        """ % {"rowkey": rowkey, 'recived_url_send': recived_url_send, "date": recived_time,
                               "status": recived_status}
        logging.info(sql_mysql)
        cursor.execute(sql_mysql)
        conn.commit()
    elif len(messagelist) < 2 or len(messagelist) > 12:
        logging.error("message is error")
        logging.error(recived_message)
    elif len(messagelist) > 2 and len(messagelist) <= 12:
        recived_url_send = messagelist[0]
        recived_status = messagelist[1]
        recived_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        recived_results = ['', '', '', '', '', '', '', '', '', '']
        rowkey = hashlib.md5(recived_url_send).hexdigest() + datetime.datetime.now().strftime('%Y%m%d%H%M%S') \
                 + recived_url_send
        logging.info(rowkey)
        logging.info("list lenth:::" + str(len(messagelist)))
        for i in range(2, len(messagelist)):
            recived_results[i - 2] = messagelist[i]
        sql_mysql = """INSERT INTO ODS_MSFACEREC_RECIVED(RowSets,send_url,recived_time,status,result_1,result_2,result_3
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
        logging.info(sql_mysql)
        cursor.execute(sql_mysql)
        conn.commit()


if __name__ == "__main__":
    bootstrap_servers = CF.get("kafka","bootstrap").split(",")
    kafka_topic = CF.get("kafka","topic")
    kafka_group_id = CF.get("kafka","group_id")
    kafka_consumer = KafkaConsumer(kafka_topic,group_id=kafka_group_id,bootstrap_servers=bootstrap_servers)
    phoenix_url = CF.get("phoenix","url")
    phoenix_conn = phoenixdb.connect(phoenix_url,autocommit=True)
    phoenix_cursor = phoenix_conn.cursor()

    mysql_conn = pymysql.connect(host=CF.get("mysql","ip"),port= int(CF.get("mysql","port")),\
                                 user = CF.get("mysql","username"),\
                                 passwd=CF.get("mysql","password"),\
                                 db=CF.get("mysql","database"),charset='utf8')
    mysql_cursor = mysql_conn.cursor()

    for message in kafka_consumer:
        logging.debug("Recived message value from kafka topic msreply" + message.value)
        #通过phoenix 插入数据到hbase
        if CF.get("phoenix","enable") == "True":
            safeToPhoenix(data=message.value,cursor=phoenix_cursor)
            print "safe to phoenix doen"
        else:
            logging.debug("safe to Phoenix is no enable")
        #插入数据到mysql
        if CF.get("mysql","enable") == "True":
            safeToMysql(message.value,cursor=mysql_cursor,conn=mysql_conn)
        else:
            logging.debug("safe to mysql is no enable")



