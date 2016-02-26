#coding=utf8
import sys
import os
from utils import *
sys.path.append("../common_tools")
from statutil import *
import time

def getMaxTid(path):
    maxTid = 0
    fin = open(path)
    for line in fin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        tid = int(ll[0])
        if(tid > maxTid):
            maxTid = tid
    fin.close()
    print "maxTid in file is %s"%(maxTid)
    return maxTid

def getCataDict():
    cataDict = {}
    conn = get_config_db('athena', 0)
    sql = "select cid, name from category where status=1"
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        cataDict[row["cid"]] = row["name"].lower().encode("utf8");
    conn.close()
    return cataDict

def getIncreForward(path, maxTid):
    cataDict = getCataDict()
    conn = get_config_db('brd_goods', 0)
    sql = "select twitter_id, shop_id, sort_id from brd_goods_info where twitter_id>%s"%(maxTid) 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    fout = open(path, "a")
    for row in rows:
        sort_id = row["sort_id"]
        if(sort_id == "None" or sort_id not in cataDict):
            continue
        print >> fout, "%s\t%s\t%s\t%s"%(row["twitter_id"], row["shop_id"], cataDict[sort_id], "-")
    conn.close()

if __name__ == '__main__':
    path = sys.argv[1]
    maxTid = getMaxTid(path)
    print maxTid
    getIncreForward(path, maxTid)


