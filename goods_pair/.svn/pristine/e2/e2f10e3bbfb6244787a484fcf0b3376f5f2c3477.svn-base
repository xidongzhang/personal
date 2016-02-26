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

def initCataDict(path):
    cDict = {}
    fin = open(path)
    for line in fin:
        line = line.strip()
        if(line == ""):
            continue
        ll=line.split("\t")
        if(ll[2] != "-"):
            cDict[ll[2]] = ll
        elif(ll[1] != "-"):
            cDict[ll[1]] = ll
        else:
            cDict[ll[0]] = ll
    fin.close()
    return cDict

def getFeaNum(metaDict):
    num = 0
    for fea in metaDict:
        if(metaDict[fea] == "1"):
            num += 1
    return num

def processColor(vDict1, vDict2, color1, color2):   
    if(color1 != "-"):
        vDict1["goods_color"] = color1
    if(color2 != "-"):
        vDict2["goods_color"] = color2

def getCata(cataStr):
    cataArr = cataStr.split("{/c}")
    #if(len(cataArr) != 3):
    #    return "-"
    #print cataStr
    if(len(cataArr) >= 3 and cataArr[2] != "-:-"):
        return cataArr[2][cataArr[2].find(":")+1:]
    if(len(cataArr) >= 2 and cataArr[1] != "-:-"):
        return cataArr[1][cataArr[1].find(":")+1:]
    #if(len(cataArr[0]) != "-"):
    return cataArr[0][cataArr[0].find(":")+1:]

def getBaseCata(cata, cataDict):
    if(cata not in cataDict):
        return "-"
    cataList = cataDict[cata]
    if(cataList[2] != "-"):
        return cataList[1]
    else:
        return cataList[0]

def processCata(vDict1, vDict2, cataDict):
    cataStr1 = vDict1["cata"]
    cataStr1 = cataStr1.lower()
    cata1 = getCata(cataStr1)
    if(cata1 in cataDict):
        vDict1["cata"] = "{/c}".join(cataDict[cata1])
    else:
        vDict1["cata"] = "-"
    cataStr2 = vDict2["cata"]
    cataStr2 = cataStr2.lower()
    baseCata1 = getBaseCata(cata1, cataDict)
    cata2 = getCata(cataStr2)
    if(cata2 in cataDict):
        vDict2["cata"] = "{/c}".join(cataDict[cata2])
    else:
        vDict1["cata"] = "-"
    baseCata2 = getBaseCata(cata2, cataDict)
    return cata1, cata2, baseCata1, baseCata2

def getTagList(tagStr):
    if(tagStr == "-" or tagStr == "NULL"):
        return "-"
    tagStr = tagStr.replace("，",",")
    tagStr = tagStr.replace(" ",",")
    tagList = tagStr.split(",")
    return tagList

def processTag(vDict1, vDict2):
    tagStr1 = vDict1["goods_tag"]
    tagList = getTagList(tagStr1)
    vDict1["goods_tag"] = "{/c}".join(tagList)
    tagStr2 = vDict2["goods_tag"]
    tagList = getTagList(tagStr2)
    vDict2["goods_tag"] = "{/c}".join(tagList)


if __name__ == '__main__':
    #input_file = os.environ['map_input_file']
    #arr = input_file.split("/")
    #inputPath = arr[len(arr)-2]
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    cataDict = initCataDict("cata_dict")
    canCompDict = initDict("can_comp_cata_dict", 2, -1)
    feaNum = getFeaNum(metaDict)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        #if len(ll) > 1:
        #    print ll
        #    continue
        #if len(ll) == 1:
        #    ll = line.split("\001")
        #    print ll
        #    continue
        vDict1 = parseLine(ll[6:6+feaNum], metaList, metaDict)
        vDict2 = parseLine(ll[6+feaNum:], metaList, metaDict)
        tid1 = ll[0]
        tid2 = ll[1]
        sign = ll[2]
        color1 = ll[3]
        color2 = ll[4]
        pv = ll[5]
        #if(inputPath != "pair_merge_fea2"):
        #    sign += "_no"
        processColor(vDict1, vDict2, color1, color2)
        #processTag(vDict1, vDict2)
        #最小类目1,2 and 上一级类目1,2   
        cata1, cata2, baseCata1, baseCata2 = processCata(vDict1, vDict2, cataDict)
        #print "%s\t%s\t%s\t%s"%(cata1, cata2, baseCata1, baseCata2)
        feaList = []
        for fea in metaList:
            if(metaDict[fea] == "1"):
                feaList.append(vDict1[fea])
        for fea in metaList:
            if(metaDict[fea] == "1"):
                feaList.append(vDict2[fea])
        
        pList = [0, 0, 0, 0]   #comp_no subti_no comp subti
        if(sign == "comp"):
            if(vDict1["cata"] == vDict2["cata"] and cata1 not in canCompDict and cata2 not in canCompDict):
                continue
            pList[2] = pv
        elif(sign == "subti"):
            if(baseCata1 != baseCata2):
                continue
            pList[3] = pv
        elif(sign == "no"):
            pList[0] = pv
            pList[1] = pv
        elif(sign == "com_no"):
            pList[0] = pv
        elif(sign == "subti_no"):
            pList[1] = pv
        print "%s\t%s"%("_::_".join(feaList), "\t".join(map(str, pList)))
        





