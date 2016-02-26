#coding=utf8
import sys
import os
import time

if __name__ == '__main__':

    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\001")
        numDict = {}
        tagStr=ll[18].strip().lower()
        if(tagStr == ""):
            continue
        tagStr=tagStr.replace("，",",") 
        tagStr=tagStr.replace(" ",",")
        tagStr=tagStr.replace("\t",",")
        tagStr=tagStr.replace("\n",",")
        arr=tagStr.split(",")
        for tag in arr:
            if(tag=="" or len(tag)>16):
                continue
            if(tag not in numDict):
                numDict[tag]=0
            numDict[tag]+=1
        for word in numDict:
            if(word!="null"):
                print "%s\t%d\t1"%(word,numDict[word])
         













