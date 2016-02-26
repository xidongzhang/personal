#coding=utf8
import sys
import os
import time
from utils import *

def processCata(cataStr):
    cataStr = cataStr.lower()
    cataArr = cataStr.split("{/c}")
    if(len(cataArr) != 3):
        return "-"
    if(cataArr[2] != "-:-"):
        return cataArr[2][cataArr[2].find(":")+1:]
    if(cataArr[1] != "-:-"):
        return cataArr[1][cataArr[1].find(":")+1:]
    return cataArr[0][cataArr[0].find(":")+1:]
   

if __name__ == '__main__':
    input_file = os.environ['map_input_file']
    arr = input_file.split("/")
    inputPath = arr[len(arr)-2]
    brdDict = initDict("verify_tid", 0, -1)
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        tid = ll[0]
        if(tid not in brdDict):
            continue
        shopid = ll[1]
        cata = ll[2]
        if(cata == "-" or cata == ""):
            continue
        tagStr = ll[3]
        print "shop_id{/c}%s\t%s"%(shopid, tid)
        print "cata{/c}%s\t%s"%(cata, tid)
        arr = tagStr.split("{/c}")
        for tag in arr:
            print "tag{/c}%s:%s\t%s"%(cata, tag, tid)





