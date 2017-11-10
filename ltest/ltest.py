# -*- coding: utf-8 -*-
"""
mysql test
"""
import datetime
import jpype
import jaydebeapi
from time import sleep

phoenix_client_jar="/opt/cloudera/phoenix-4.9.0-cdh5.9.1/phoenix-4.9.0-cdh5.9.1-client.jar"
args='-Djava.class.path=%s' % phoenix_client_jar
jvm_path=jpype.getDefaultJVMPath()
jpype.startJVM(jvm_path,args)
conn=jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                        'jdbc:phoenix:jp-bigdata-03:2181',
                        [],
                        phoenix_client_jar)
curs=conn.cursor()
while True:
    time_test = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    curs.execute("UPSERT INTO test.LTEST VALUES (?, ?)", (time_test, time_test))
    conn.commit()
    print "save ok " + str(datetime.datetime.now())
    sleep(2)