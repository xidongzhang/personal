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
        if(inputPath == "goods_fea_tag"):
            ll = line.strip().split("\t")
            vDict = parseLine(ll, metaList)
            tid = vDict["twitter_id"]
            pList = []
            for i in range(len(metaList)):
                meta = metaList[i]
                if(metaDict[meta] == "0"):
                    continue
                pList.append(vDict[meta])
            #print "goods_fea_tag"
            print "%s\t0\t%s"%(tid, "\t".join(pList))
        else:
            ll = line.strip().split("\001")
            tid1 = ll[0]
            #print tid1
            print "%s\t1\t%s"%(tid1, "\t".join(ll))
            



