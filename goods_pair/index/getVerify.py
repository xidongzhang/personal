#coding=utf8
import sys
import os
from utils import *
sys.path.append("../common_tools")
from statutil import *
import time

def getVerifyTid(path):
    conn = get_config_db('fashion', 0)
    sql = "select twitter_id from t_fashion_twitter_verify_operation where verify_stat=5 and second_score>1 and third_score>0 and twitter_id in (%s)"
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    fin = open(path)
    tidList = []
    for line in fin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        tid = ll[0]
        if(len(tidList) >= 1000):
            tidStr = ",".join(tidList)
            eSql = sql%(tidStr)
            cursor.execute(eSql)
            rows = cursor.fetchall()
            tidList = []
            for row in rows:
                print row["twitter_id"]
        tidList.append(tid)
    fin.close()
    if(len(tidList) > 0):
        tidStr = ",".join(tidList)
        eSql = sql%(tidStr)
        cursor.execute(eSql)
        rows = cursor.fetchall()
        for row in rows:
            print row["twitter_id"]
    conn.close()

if __name__ == '__main__':
    path = sys.argv[1]
    getVerifyTid(path)

