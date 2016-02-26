#coding=utf8
import sys
import os
import time
from utils import *

def getFeaNum(metaDict):
    num = 0
    for fea in metaDict:
        if(metaDict[fea] == "1"):
            num += 1
    return num

def parseLine(ll, metaList, metaDict):
    vDict = {}
    index = 0
    for i in range(len(metaList)):
        if(metaDict[metaList[i]] == "1"):
            vDict[metaList[i]] = ll[index]
            index += 1
    return vDict

def getShowClick(ll, sign, sample):
    noNum = float(ll[0])
    subtiNum = float(ll[2])
    compNum = float(ll[1])
    show = 0
    click = 0
    if(sign == "subti"):
        show = noNum 
        click = subtiNum
    elif(sign == "comp"):
        show = noNum 
        click = compNum
    s = 0
    for i in range(int(show)):
        r=random.randint(1,100)
        if(r <= sample):
            s += 1
    show = s
    if(click > 0 and show <= click):
        show += click
        if(show == click):
            show += 1
    return show, click


if __name__ == '__main__':
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    brdDict = initDict("brd_goods", 0, -1)
    feaStart = 4
    feaNum = getFeaNum(metaDict)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        noCompNum = float(ll[0])
        noSubtiNum = float(ll[1])
        compNum = float(ll[2])
        subtiNum = float(ll[3])
        if(compNum < 4 and subtiNum < 4):
            continue
        
        vDict1 = parseLine(ll[feaStart:feaStart+feaNum], metaList, metaDict)
        vDict2 = parseLine(ll[feaStart+feaNum:], metaList, metaDict)
        tid1 = vDict1["twitter_id"]    
        tid2 = vDict2["twitter_id"]   
        if(tid1 not in brdDict or tid2 not in brdDict): 
            continue
        if(compNum >= 4):
            print "2{/c}%s\t%s"%(tid1, tid2)
            #print "comp{/c}%s\t%s"%(tid1, tid2)
        if(subtiNum >= 4):
            print "1{/c}%s\t%s"%(tid1, tid2)
            #print "subti{/c}%s\t%s"%(tid1, tid2)

        





