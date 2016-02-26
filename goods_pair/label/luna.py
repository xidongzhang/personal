#coding=utf8
import sys
import os
from utils import *
sys.path.append("../common_tools")
from statutil import *
import time

def getTidDict(tidDict, goodsidList, conn):
    sql = "select goods_id,twitter_id from brd_goods_info where goods_id in (%s)"%(",".join(map(str,goodsidList))) 
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        tidDict[row["goods_id"]] = row["twitter_id"]

def tranGoodsId(resultDict):
    tidDict = {}
    goodsidList = []
    conn = get_config_db('brd_goods', 0)
    for dressid in resultDict:
        for goodsid in resultDict[dressid]:
            goodsidList.append(goodsid)
            if(len(goodsidList) >= 1000):
                getTidDict(tidDict, goodsidList, conn)
                goodsidList = []
    if(len(goodsidList) > 0):
        getTidDict(tidDict, goodsidList, conn)
    conn.close()

    tidResultDict = {}
    for dressid in resultDict:
        tidResultDict[dressid] = []
        for goodsid in resultDict[dressid]:
            if(goodsid in tidDict):
                tidResultDict[dressid].append(tidDict[goodsid])
    return tidResultDict

def getLuna(lastId, result_file):
    sql = "select dress_id,goods_id from t_dress_goods_twitter where type=1 and dress_id > %s"%(lastId)
    print sql
    conn = get_config_db('club', 0)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    maxId = 0
    resultDict={}
    for row in rows:
        dressid = row["dress_id"]
        goods_id = row["goods_id"]
        if(dressid > maxId):
            maxId = dressid
        if(dressid not in resultDict):
            resultDict[dressid] = []
        resultDict[dressid].append(goods_id)
    conn.close()

    resultDict = tranGoodsId(resultDict)
    usedDict = {}
    fout = open(result_file, "w")
    idx = 0
    for dressid in resultDict:
        idx += 1
        if(idx % 5000 == 0):
            print "luna output %s group"%(idx)
        for i in range(len(resultDict[dressid])):
            for j in range(len(resultDict[dressid])):
                if(i == j):
                    continue
                key = "%s\t%s"%(resultDict[dressid][i], resultDict[dressid][j])
                if(key not in usedDict):
                #dressid, tid1, tid2, ctime, price, color, size
                    print >> fout, "luna\t%s\t1\t%s\t-\t-\t-\t-\t-\t-\t-\t-"%(dressid, key)
                    usedDict[key] = 1
    fout.close()
    return maxId


def getLastId(idFile):
    fin = open(idFile)
    lastId = 0
    for line in fin:
        line = line.strip()
        if(line == ""):
            continue
        lastId = line
    fin.close()
    return int(lastId)

def writeMaxId(idFile, maxId, lastId):
    if(maxId <= lastId):
        return
    fout = open(idFile, "w")
    print >> fout, maxId
    fout.close()


if __name__ == '__main__':
    root_path=sys.argv[1]
    idFile = "last_dress_id"
    cmd = "rm luna_file_tmp"
    os.system(cmd)
    #lastId = getLastId(idFile)
    lastId = 0
    maxId = getLuna(lastId, "luna_file_tmp")
    #writeMaxId(idFile, maxId, lastId)   
    #fout = open("luna_file", "a")
    fout = open("luna_file", "w")
    fin = open("luna_file_tmp")
    for line in fin:
        line=line.strip()
        print >> fout,line
    fin.close()
    fout.close()

    hdfsPath = "%s/label_data/comp_from_luna/"%(root_path)
    cmd = "hadoop fs -rmr %s"%(hdfsPath)
    os.system(cmd)
    cmd = "hadoop fs -mkdir %s"%(hdfsPath)
    os.system(cmd)
    cmd = "hadoop fs -put luna_file %s"%(hdfsPath)
    os.system(cmd)


