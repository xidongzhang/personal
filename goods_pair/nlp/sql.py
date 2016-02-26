# -*- coding: UTF-8 -*-
import sys
sys.path.append("../common_tools")
from statutil import *
import MySQLdb

if __name__ == '__main__':

    db = sys.argv[1]
    sql = sys.argv[2]
    selectStr = sys.argv[3]

    selectArr = selectStr.split(",")
    conn = get_config_db(db,0)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        pList=[]
        for s in selectArr:
            res=row[s]
            try:
                res=str(res)
            except:
                pass
            res=res.strip()
            pList.append(res.encode("utf8"))
        print "\t".join(pList)
    conn.close()


        
        
