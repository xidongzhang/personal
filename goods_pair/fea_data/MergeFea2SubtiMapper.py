#coding=utf8
import sys
import os
import time
from utils import *

def parseLine(ll, metaList):
    vDict = {}
    for i in range(len(metaList)):
        vDict[metaList[i]] = ll[i]
    return vDict

if __name__ == '__main__':
    input_file = os.environ['map_input_file']
    arr = input_file.split("/")
    inputPath = arr[len(arr)-2]
    metaList = initList("goods_meta", 0)
    metaDict = initDict("goods_meta", 0, 1)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.strip().split("\t")
        if(inputPath == "test_subti_pair_merge_fea1"):
            tid2 = ll[1]
            print "%s\t1\t%s"%(tid2, line)
        else:
            vDict = parseLine(ll, metaList)
            tid = vDict["twitter_id"]
            pList = []
            for i in range(len(metaList)):
                meta = metaList[i]
                if(metaDict[meta] == "0"):
                    continue
                pList.append(vDict[meta])
            print "%s\t0\t%s"%(tid, "\t".join(pList))

        
        

        
         













