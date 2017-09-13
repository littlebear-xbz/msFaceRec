# -*- coding: utf-8 -*-
# ----------------------------------------------------------------
# 功能：实时统计ip地址数量
# 实现：flume 获取 日志记录，并将数据推送至kafka中
#       spark streaming 按RDD处理数据
#       处理后数据写入hbase中进行统计
# 编写人： chenyangang
# 日期： 2017-09-13
# ----------------------------------------------------------------
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import datetime

import phoenixdb

class WriteToHBase(object):
    cursor = None
    cnx = None

    def __init__(self):
        self.initConnect()

    def initConnect(self):
        database_url = 'http://jp-bigdata-01:8765/'

        try:
            conn = phoenixdb.connect(database_url, autocommit=True)
            print("-------------------Sucessful--------------------------")
        except phoenixdb.Connection as err:
            print("Failed creating database: {}".format(err))
            exit(1)

        self.cursor = conn.cursor()
        self.cnx = conn
        return self.cursor, self.cnx

    def upsertTable(self, login_cnt, statis_date, statis_hour, ipaddr):
        update_static_result = ("UPSERT INTO  ads_log_static_ipaddr"
                                "(statis_date, statis_hour, ipaddr, login_cnt) "
                                "VALUES (%s, %s,  %s, %d) "
                                "ON DUPLICATE KEY UPDATE login_cnt = login_cnt + %d")
        self.cursor.execute(update_static_result, (statis_date, statis_hour, ipaddr, login_cnt, login_cnt))

    def insertDetail(self, ipaddr, user_id, login_time, url, write_time):
        add_detail_log = (" INSERT INTO dwd_log_login_d "
                          "(ipaddr, user_id, login_time, url, write_time)"
                          "VALUES (%s, %s,  %s, %s, %s)")
        self.cursor.execute(add_detail_log, (ipaddr, user_id, login_time, url, write_time))


def initSparkStreaming():
    sc = SparkContext("local[7]", "KafkaWordCount")
    ssc = StreamingContext(sc, 2)
    # set zookeeper
    zookeeper = "jp-bigdata-02:2181, jp-bigdata-03:2181, jp-bigdata-06:2181"

    # set kafka's topic
    topic = "test_flume_kafka"

    initDstream = KafkaUtils.createStream(ssc=ssc, zkQuorum=zookeeper, groupId="kafka-streaming-redis",
                                          topics={topic: 1})
    return ssc, initDstream

def processRDD(rdd):
    initRDD = rdd.map(lambda x: x[1]).map(lambda line: line.split(" "))
    staticRDD = initRDD.map(lambda x: x[0]).map(lambda ipaddr: (ipaddr, 1)) \
        .reduceByKey(lambda a, b: a + b)

    detailRDD = initRDD.map(lambda line: {'ipaddr': line[0],\
                                          'user_id': line[2],\
                                          'login_time': line[3][1:],\
                                          'url': line[10] \
                                          })
    return staticRDD, detailRDD


def main():
    # 连接spark streaming流
    ssc, initDstream = initSparkStreaming()

    # 处理RDD
    staticRDD, detailRDD = processRDD(initDstream)

    # 数据写入redis中并进行计算
    r = WriteToHBase()

    # 统计ip地址次数,并将统计结果写入mysql
    def ipHandle(time ,rdd):
        statis_date = datetime.datetime.now().strftime("%Y-%m-%d")
        statis_hour = datetime.datetime.now().strftime("%H")

        if rdd.isEmpty() is False:
            # rddstr = "{"+','.join(rdd.collect())+"}"
            for element in rdd.collect():
                r.upsertTable(statis_date, statis_hour, element[0], element[1])

    staticRDD.foreachRDD(ipHandle)

    # 明细数据写入redis和mysql
    def handleItem(time, rdd):
        dealtime=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if rdd.isEmpty() is False:
            for element in rdd.collect():
                r.writeToMySQL.insertDetail(element['ipaddr'], element['user_id'], element['login_time'], element['url'], dealtime)

    detailRDD.foreachRDD(handleItem)

    # 处理结束，关闭streaming
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()