# -*- coding: utf-8 -*-

import ConfigParser
import time
cf = ConfigParser.ConfigParser()
cf.read('../conf/conf.conf')

print cf.get('phoenix','url')
a = cf.get("kafka","bootstrap")
server_list = a.split(",")
print server_list

while True:
    print cf.get("phoenix","enable")
    time.sleep(2)