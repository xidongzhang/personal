#coding=utf8
import sys
sys.path.append("../common_tools")
from statutil import *

def getHash(value):
    h = 0;  
    if (h == 0 and len(value) > 0):
        for i in range(len(value)):
            h = 31 * h + ord(value[i])
        hash = h 
    return h


if __name__ == '__main__':
    conn = get_config_db('athena',0)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    sql = "select name from category"
    cursor.execute(sql)
    rows=cursor.fetchall()
    for row in rows:
        cataStr = row["name"]
        arr = cataStr.split("/")
        for cata in arr:
            print cata.encode("utf8").lower()
    conn.close()

     
    
