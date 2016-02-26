#coding=utf8
import sys
import os
import time
import json
from utils import *
from segUtils import *
import zipimport

def parseLineFromConf(line,confList,separator="\t"):
    ll=line.split(separator)
    vDict={}
    if(len(ll)!=len(confList)):
        return vDict
    idx=0
    for feaName in confList:
        vDict[feaName]=ll[idx]
        idx+=1
    return vDict

def segWords(line):
    rList=[]
    line=line.strip().lower()
    if(line==""):
        return rList
    str_list = list(jieba.cut(line))
    #for t in Seg(line):
    for t in str_list:
        rList.append(t[0])
    return rList

def processTitle(title,keyWordsDict):
    wList=segWords(title)
    resArr=wordsJoin(wList,keyWordsDict,True)
    return resArr

def processTag(tagStr,keyWordsDict):
    resArr=[]
    tagStr=tagStr.replace("，",",")
    tagStr=tagStr.replace(" ",",")
    tagStr=tagStr.replace("\t",",")
    tagStr=tagStr.replace("\n",",")
    arr=tagStr.split(",")
    for t in arr:
        if(t in keyWordsDict):
            resArr.append(t)
    return resArr

def processStyle(styleStr,keyWordsDict):
    resArr=[]
    arr=styleStr.split("{/c}")
    for sStr in arr:
        p=sStr.find("_")
        if(p>0):
            sStr=sStr[p+1:]
        if(sStr in keyWordsDict):
            resArr.append(sStr)
    return resArr

if __name__ == '__main__':
    input_file = os.environ['map_input_file']
    arr=input_file.split("/")
    inputPath=arr[len(arr)-2]
    #inputPath="goods_attr_format"
    #inputPath="action_merge"
    #inputPath=sys.argv[1]
    #curtime=time.time()
    importer = zipimport.zipimporter('jieba.mod')
    jieba = importer.load_module('jieba')
    jieba.set_dictionary('dict.txt')
    #metaList=initList("goods_meta",0)
    confList=initList('conf.txt',0)
    needMetaList=initList("goods_need_meta",0)
    keyWordsDict=initDict("title_word_count_sort",0,-1) 
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        if(inputPath=="action_merge"):
            ll=line.split("\t")
            if(len(ll)!=6):
                continue
            uid=ll[0]
            tid=ll[1]
            try:
                if(int(tid)>2061202555):
                    print "%s\t2\t%s\t%s"%(tid,uid,"\t".join(ll[2:]))
            except:
                pass
        elif(arr[len(arr)-3]=="ml_goods_info"):
            #print line
            llDict=parseLineFromConf(line, confList)
            if(len(llDict)==0):
                continue
            tid=llDict["twitter_id"]
            gid=llDict["goods_id"]
            titleWordList=processTitle(llDict["goods_title"],keyWordsDict)
            tagWordList=processTag(llDict["goods_tag"],keyWordsDict)
            styleWordList=processStyle(llDict["goods_style_for_ctr"],keyWordsDict)
            styleDict={}
            for w in titleWordList:
                styleDict[w]=1
            for w in tagWordList:
                styleDict[w]=1
            for w in styleWordList:
                styleDict[w]=1
            sList=[]
            for w in styleDict:
                sList.append(w)
            if(len(sList)==0):
                llDict["style"]="NULL"
            else:
                llDict["style"]="{/c}".join(sList)
            printList=[]
            for meta in needMetaList[1:]:
                printList.append(llDict[meta])
            print "%s\t1\t%s"%(tid,"\t".join(printList))
















