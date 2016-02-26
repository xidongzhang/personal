#coding=utf8
import sys
import os
import time
import random
from utils import *

def parseLine(ll, metaList):
    vDict = {}
    for i in range(len(metaList)):
        vDict[metaList[i]] = ll[i]
    return vDict

if __name__ == '__main__':
    #input_file = os.environ['map_input_file']
    #arr = input_file.split("/")
    #inputPath = arr[len(arr)-3]
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        vDict = parseLine(ll, metaList)
        tid = vDict["twitter_id"]
        pList = []
        for i in range(len(metaList)):
            meta = metaList[i]
            if(metaDict[meta] == "0"):
                continue
            pList.append(vDict[meta])
        
        r = random.randint(1,150000)
        print "%s\t%s\t%s"%(r, tid, "\t".join(pList))
             
        
        

        
         













