#coding=utf8
import sys
import os
from utils import *

def initPvDict(path):
    pvDict = {}
    fin = open(path)
    for line in fin:
        line = line.strip()
        if(line == ""):
            continue
        ll=line.split("\t")
        arr=ll[0].split("{/c}")
        if(arr[0] == "freq"):
            if(arr[1] == "-"):
                continue
            pvDict[arr[1]] = ll[1]
    fin.close()
    return pvDict


if __name__ == '__main__':
    pvDict = initPvDict("comp_cata")
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        arr=ll[0].split("{/c}")
        if(arr[0] != "co"):
            continue
        cata1 = arr[1]
        cata2 = arr[2]
        if(cata1 == "-" or cata2 == "-"):
            continue
        if(cata1 not in pvDict or cata2 not in pvDict):
            continue
        if((cata1.find("男") >= 0 and cata2.find("男") < 0) or (cata1.find("男") < 0 and cata2.find("男") >= 0)):
            continue
        pv1 = pvDict[cata1]
        pv2 = pvDict[cata2]
        co = ll[1]
        print "%s\t%s\t%s\t%s\t%s"%(cata1, cata2, co, pv1, pv2)



