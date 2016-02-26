#coding=utf8
import sys
import os
import time
from utils import *

def parseLine(ll, metaList):
    vDict = {}
    for i in range(len(metaList)):
        vDict[metaList[i]] = ll[i]
    return vDict

def processCata(cataStr):
    cataStr = cataStr.lower()
    cataArr = cataStr.split("{/c}")
    if(len(cataArr) >=3 and cataArr[2] != "-:-"):
        return cataArr[2][cataArr[2].find(":")+1:]
    if(len(cataArr) >=2 and cataArr[1] != "-:-"):
        return cataArr[1][cataArr[1].find(":")+1:]
    return cataArr[0][cataArr[0].find(":")+1:]
    

if __name__ == '__main__':
    input_file = os.environ['map_input_file']
    arr = input_file.split("/")
    inputPath = arr[len(arr)-2]
    metaList = initList("goods_meta", 0)
    #brdDict = initDict("brd_goods", 0, -1)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        vDict = parseLine(ll, metaList)
        tid = vDict["twitter_id"]
        if(int(tid) < 1187609789):
            continue
        #if(tid not in brdDict):
        #    continue
        shopid = vDict["shop_id"]
        cataStr = vDict["cata"]
        tagStr = vDict["tag"]
        cata = processCata(cataStr)
        if(cata == "-" or cata == ""):
            continue
        print "%s\t%s\t%s\t%s"%(tid, shopid, cata, tagStr)






