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
import time
import datetime
import ConfigParser
import json
import threading
CF = ConfigParser.ConfigParser()
CF.read('../conf/conf.conf')

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='../log/saveOriUrlToPhoenix.log',
                    filemode='w'
                    )


def saveToPhoenix(data, cursor, conn):
    message = data
    if message == 'heart beat':
        cursor.execute("select 1")
        print "heart beat"
        logging.debug("phoenix heart beat success")
        return "heart beat"
    else:
        ods_data = json.loads(message)
        image_num = str(ods_data["ImgNum"])
        camera_id = str(ods_data["CameraId"])
        camera_ip = str(ods_data["CameraIp"])
        time_send = str(ods_data["PassTime"])
        time_recive = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        face_url = str(ods_data["FaceUrl"])
        RowSets = hashlib.md5(time_recive + face_url).hexdigest() + "|" + time_recive + "|" + face_url
        sql = """UPSERT INTO ods.ODS_FACE_PICTURE(RowSets,image_num,camera_id,camera_ip,time_send,time_recive,face_url)
                    VALUES('%(RowSets)s',
                    '%(image_num)s',
                    '%(camera_id)s',
                    '%(camera_ip)s',
                    '%(time_send)s',
                    '%(time_recive)s',
                    '%(face_url)s'
                    )
                    """ % {"RowSets": RowSets, "image_num": image_num, "camera_id": camera_id,
                           "camera_ip": camera_ip ,"time_send": time_send,"time_recive": time_recive
                           ,"face_url": face_url
                           }
        logging.info("insert into phoenix" + sql)
        cursor.execute(sql)

def kafkaToDb(phoenix_cursor, phoenix_conn):
    bootstrap_servers = CF.get("kafka", "bootstrap").split(",")
    kafka_topic = CF.get("kafka", "topic_send")
    kafka_group_id = CF.get("kafka", "group_id")
    kafka_consumer = KafkaConsumer(
        kafka_topic, group_id=kafka_group_id, bootstrap_servers=bootstrap_servers)

    for message in kafka_consumer:
        print message.value
        logging.debug(
            "Recived message value from kafka topic ::" + message.value)
        # 通过phoenix 插入数据到hbase
        if CF.get("phoenix", "enable") == "True":
            saveToPhoenix(data=message.value,
                          cursor=phoenix_cursor, conn=phoenix_conn)
            print "safe to phoenix done"
        else:
            logging.debug("safe to Phoenix is no enable")
        # 插入数据到mysql



def keepConn(cur):
    while True:
        cur.execute("select 1")
        logging.debug("select 1")
        print "keep alive"
        time.sleep(200)


def main():
    print "process start"
    phoenix_url = CF.get("phoenix", "url")
    phoenix_conn = phoenixdb.connect(
        phoenix_url, max_retries=3, autocommit=True)
    phoenix_cursor = phoenix_conn.cursor()
    kafkaToDb_thread = threading.Thread(
        target=kafkaToDb, args=(phoenix_cursor, phoenix_conn,))
    keepConn_thread = threading.Thread(target=keepConn, args=(phoenix_cursor,))
    kafkaToDb_thread.start()
    keepConn_thread.start()
    kafkaToDb_thread.join()
    keepConn_thread.join()


if __name__ == "__main__":
    main()
