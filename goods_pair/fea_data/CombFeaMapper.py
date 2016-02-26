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

def processCataTag(vDict1, vDict2, feaName):
    tag1 = vDict1["tag"]
    tag2 = vDict2["tag"]
    vList = []
    if(tag1 == "-" or tag1 == "NULL" or tag2 == "-" or tag2 == "NULL"):
        return vList
    cata1 = getCata(vDict1["cata"])
    cata2 = getCata(vDict2["cata"])
    tag1 = tag1.replace("&", " ")
    tag1 = tag1.replace("#", " ")
    tag2 = tag2.replace("&", " ")
    tag2 = tag2.replace("#", " ")
    arr1 = tag1.split("{/c}")
    arr2 = tag2.split("{/c}")
    for i in range(len(arr1)):
        for j in range(len(arr2)):
            vList.append("%s&%s:%s#%s:%s"%(feaName, cata1, arr1[i], cata2, arr2[j]))
    return vList

def processCata(vDict1, vDict2, feaName):
    cata1Str = vDict1["cata"]
    cata2Str = vDict2["cata"]
    vList = []
    if(cata1Str == "" or cata2Str == "" or cata1Str == "NULL" or cata2Str == "NULL"):
        return vList
    cata1 = getCata(cata1Str)
    cata2 = getCata(cata2Str)
    vList.append("%s&%s#%s"%(feaName, cata1, cata2))
    return vList
    

def processFea(vDict1, vDict2, feaName, feaSign):
    vList = []
    if(feaSign == "0"):
        vList = processSingleFea(vDict1[feaName], vDict2[feaName], feaName)
    else:
        if(feaName == "cata#tag"):
            vList = processCataTag(vDict1, vDict2, feaName)
        elif(feaName == "cata"):
            vList = processCata(vDict1, vDict2, feaName)
        else:
            pass
    return  vList

def processSingleFea(feaValue1, feaValue2, feaName):
    vList = []
    if(feaValue1 == "-" or feaValue1 == "NULL" or feaValue2 == "-" or feaValue2 == "NULL"):
        return vList
    feaValue1 = feaValue1.replace("&", " ")
    feaValue1 = feaValue1.replace("#", " ")
    feaValue2 = feaValue2.replace("&", " ")
    feaValue2 = feaValue2.replace("#", " ")
    arr1 = feaValue1.split("{/c}")
    arr2 = feaValue2.split("{/c}")
    for i in range(len(arr1)):
        for j in range(len(arr2)):
            vList.append("%s&%s#%s"%(feaName, arr1[i], arr2[j]))
    return vList

def getShowClick(ll, sign, sample):
    noCompNum = float(ll[0])
    noSubtiNum = float(ll[1])
    compNum = float(ll[2])
    subtiNum = float(ll[3])
    show = 0
    click = 0
    if(sign == "subti"):
        show = noSubtiNum
        click = subtiNum
    elif(sign == "comp"):
        show = noCompNum 
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
    needDict = initDict("goods_meta_used", 0, 1)
    sign = sys.argv[1]
    sample = int(sys.argv[2])
    feaStart = 4
    feaNum = getFeaNum(metaDict)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        show, click = getShowClick(ll[0:feaStart], sign, sample)
        if(show <= 0):
            continue

        lineSign = get_sha1("\t".join(ll[feaStart:]))
        
        vDict1 = parseLine(ll[feaStart:feaStart+feaNum], metaList, metaDict)
        vDict2 = parseLine(ll[feaStart+feaNum:], metaList, metaDict)
        for fea in needDict:
            vList = processFea(vDict1, vDict2, fea, needDict[fea])
            for v in vList:
                print "%s\t%s\t2"%(v, int(show))
                print "%s\t%s\t3"%(v, lineSign)
        
        print "%s\t%s:%s\t1"%(lineSign, show, click)



