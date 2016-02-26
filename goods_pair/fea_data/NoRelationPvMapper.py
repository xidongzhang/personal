#coding=utf8
import sys
import os
import time
import random
from utils import *

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


if __name__ == '__main__':
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    feaNum = getFeaNum(metaDict)
    sign = sys.argv[1]
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        vDict1 = parseLine(ll[6:6+feaNum], metaList, metaDict)
        vDict2 = parseLine(ll[6+feaNum:], metaList, metaDict)
        tid1 = ll[0]
        tid2 = ll[1]
        curSign = ll[2]
        if(sign != curSign):
            continue

        color1 = ll[3]
        color2 = ll[4]
        pv = int(ll[5])

        tid1FeaList = ll[6:6+feaNum]
        tid2FeaList = ll[6+feaNum:]
       
        randomLimit = 0
        rSampleLimit = 100
        if(sign == "comp"):
            randomLimit = 10000000
            rSampleLimit = 100
        if(sign == "subti"):
            randomLimit = 100000000
            rSampleLimit = 40
        for i in range(pv):
            r = random.randint(1, randomLimit)
            rSample = random.randint(1, 100)
            if(rSample > rSampleLimit):
                continue
            key1 = "%s{/c}1"%(tid1)
            key2 = "%s{/c}2"%(tid2)
            print "%s\t%s\t%s"%(r, key1, "_::_".join(tid1FeaList))
            print "%s\t%s\t%s"%(r, key2, "_::_".join(tid2FeaList))
        



