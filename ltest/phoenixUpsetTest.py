import phoenixdb
import datetime
from time import sleep
url = "http://jp-bigdata-01:8765/"

conn = phoenixdb.connect(url, max_retries=3, autocommit=True)
cursor = conn.cursor()

while True:
    time_test = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("UPSERT INTO test.LTEST VALUES (?, ?)", (time_test, time_test))
    print "save ok " + str(datetime.datetime.now())
    sleep(599)

