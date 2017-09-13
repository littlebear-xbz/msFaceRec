# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 09:28:25 2017
@author: Xiongz
"""

import phoenixdb
database_url = 'http://jp-bigdata-01:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
cursor = conn.cursor()
cnx = conn
cursor.execute("""select * from "user_log_info" limit 5""")
print cursor.fetchall()