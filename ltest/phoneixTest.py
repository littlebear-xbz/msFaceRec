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
# cursor.execute("""select * from "user_log_info" limit 5""")

# sql = """UPSERT INTO LTEST(url_send,status,date,result_1,rowkeybyme)
# VALUES(?,?,?,?,?)"""
# # ('http://139.219.102.23:8003/JojoAndPage.jpg','success','2017-07-06 ',
# # 'http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485',
# # '123http://139.219.102.23:8003/JojoAndPage.jpg')
# recivied_url_send = 'http://139.219.102.23:8003/JojoAndPage.jpg'
# recivied_status = 'success'
# recivied_data = '2017-07-06'
# recivied_result_1 = 'http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485'
# recivied_rowkey = '1http://139.219.102.23:8003/JojoAndPage.jpg'
# cursor.execute(sql ,(recivied_url_send,recivied_status,recivied_data,recivied_result_1,recivied_rowkey))
# print cursor.fetchall()

sql = """UPSERT INTO test.LTEST(RowSets,send_url,recived_time,status,result_1)
VALUES('2http://139.219.102.23:8003/JojoAndPage.jpg',
'2http://139.219.102.23:8003/JojoAndPage.jpg',
'2017-09-13 12:12:12',
'success',
'http://wh-dev:8009/JojoAndPage/JojoAndPage_1/JojoAndPage_1.jpg|C:\zhiqian\Faces\PDB\438\page.jpg|page|0.7031485'
)
"""
cursor.execute(sql)