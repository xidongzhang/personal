#coding=utf8
import sys
import os
import time
import hashlib
import random
from utils import *

def get_sha1(src):
	return hashlib.sha1(src).hexdigest()

def parseLine(ll, metaList, metaDict):
    vDict = {}
    index = 0
    for i in range(len(metaList)):
        if(metaDict[metaList[i]] == "1"):
            vDict[metaList[i]] = ll[index]
            index += 1
    return vDict

def getFeaNum(metaDict):
    num = 0
    for fea in metaDict:
        if(metaDict[fea] == "1"):
            num += 1
    return num

def getCata(cataStr):
    if(cataStr == "-" or cataStr == "NULL"):
        return "-"
    arr = cataStr.split("{/c}")
    if(len(arr) > 2 and arr[2] != "-"):
        return arr[2]
    elif(len(arr) > 1 and arr[1] != "-"):
        return arr[1]
    return arr[0]

if __name__ == '__main__':
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    feaStart = 4
    feaNum = getFeaNum(metaDict)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")

        compNum = float(ll[2])
        if(compNum <= 0):
            continue
        vDict1 = parseLine(ll[feaStart:feaStart+feaNum], metaList, metaDict)
        vDict2 = parseLine(ll[feaStart+feaNum:], metaList, metaDict)

        cataStr1 = vDict1["cata"]
        cataStr2 = vDict2["cata"]
        cata1 = getCata(cataStr1)
        cata2 = getCata(cataStr2)
        
        print "freq{/c}%s\t%s"%(cata1, compNum)
        print "freq{/c}%s\t%s"%(cata2, compNum)
        print "co{/c}%s{/c}%s\t%s"%(cata1, cata2, compNum)



