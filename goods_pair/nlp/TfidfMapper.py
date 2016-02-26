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

def process(vStr):
    if(vStr == "-" or vStr == "NULL"):
        return []
    vStr = vStr.lower()
    vList = vStr.split("{/c}")
    return vList 

def processTitle(title_splits, addiDict):
    if(title_splits == "-" or title_splits == "NULL"):
        return []
    arr = title_splits.split("{/c}")
    resArr=wordsJoin(arr, addiDict, True)
    return resArr 


if __name__ == '__main__':
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    addiDict = initDict("addi_dict", 0 ,-1)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        vDict = parseLine(ll, metaList)
        tid = vDict["twitter_id"]
        goods_tag = vDict["goods_tag"]
        goods_style_for_ctr = vDict["goods_style_for_ctr"]
        title_splits = vDict["title_splits"]
        style = vDict["style"]
        tagList = getTagList(goods_tag)
        titleList = processTitle(title_splits, addiDict)
        styleList = process(style)
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
        for w in fDict:
            print "%s\t%s\t%s"%(w, fDict[w], 1)


