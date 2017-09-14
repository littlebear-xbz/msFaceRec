import hashlib
import datetime
database_url = 'http://jp-bigdata-01:8765/'
setMd5 = hashlib.md5(database_url).hexdigest()
print setMd5 + datetime.datetime.now().strftime('%Y%m%d%H%M%S')