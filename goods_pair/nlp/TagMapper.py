#coding=utf8
import sys
import os
import time
from utils import *
from segUtils import *

def parseLine(ll, metaList):
    vDict = {}
    for i in range(len(metaList)):
        vDict[metaList[i]] = ll[i]
    return vDict

def getTagList(tagStr):
    if(tagStr == "-" or tagStr == "NULL"):
        return "-"
    tagStr = tagStr.lower()
    tagStr = tagStr.replace("，",",")
    tagStr = tagStr.replace(" ",",")
    tagList = tagStr.split(",")
    return tagList

def processStyle(vStr, addiDict):
    if(vStr == "-" or vStr == "NULL"):
        return []
    vStr = vStr.lower()
    arr = vStr.split("{/c}")
    vList = []
    for v in arr:
        p = v.find("_")
        feaValue = v[p+1:]
        if(feaValue in addiDict):
            vList.append(feaValue)
    return vList 

def processTitle(title_splits, addiDict):
    if(title_splits == "-" or title_splits == "NULL"):
        return []
    arr = title_splits.split("{/c}")
    resArr=wordsJoin(arr, addiDict, True)
    return resArr 

def tagCmp(x,y):
    if(x[1] > y[1]):
        return -1
    if(x[1] < y[1]):
        return 1
    if(x[2] > y[2]):
        return -1
    if(x[2] < y[2]):
        return 1
    return 0

def getGoodsTag(fDict, addiDict, cataDict):
    sortList = []
    for w in fDict:
        if(w not in addiDict or w in cataDict):
            continue
        sortList.append((w, fDict[w], float(addiDict[w])))
    sortList.sort(cmp=tagCmp)
    sortList = sortList[0:10]
    rList = []
    for tpl in sortList:
        #rList.append("%s:%s:%s"%(tpl[0],tpl[1],tpl[2]))
        rList.append("%s"%(tpl[0]))
    return rList

if __name__ == '__main__':
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    addiDict = initDict("addi_word", 0, 2)
    cataDict = initDict("cata_dict", 0, -1)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        if(len(ll) < len(metaList)):
            continue
        vDict = parseLine(ll, metaList)
        tid = vDict["twitter_id"]
        goods_tag = vDict["goods_tag"]
        goods_style_for_ctr = vDict["goods_style_for_ctr"]
        title_splits = vDict["title_splits"]
        goods_style_for_ctr = vDict["goods_style_for_ctr"]
        tagList = getTagList(goods_tag)
        titleList = processTitle(title_splits, addiDict)
        styleList = processStyle(goods_style_for_ctr, addiDict)
        fDict = {}
        for w in tagList:
            if(w not in fDict):
                fDict[w] = 0
            fDict[w] += 1
        for w in titleList:
            if(w not in fDict):
                fDict[w] = 0
            fDict[w] += 1
        for w in styleList:
            if(w not in fDict):
                fDict[w] = 0
            fDict[w] += 1
        goodsTagList = getGoodsTag(fDict, addiDict, cataDict)
        if(len(goodsTagList) == 0):
            goodsTagList = ["-"]
        #print "%s\t%s\t%s\t%s\t%s\t%s"%(tid, goods_tag, goods_style_for_ctr, title_splits, "{/c}".join(styleList), "{/c}".join(goodsTagList))
        print "%s\t%s"%(line, "{/c}".join(goodsTagList))


