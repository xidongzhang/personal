#coding=utf8
import sys
import os
from utils import *

def initPvDict(path):
    rDict={}
    fin=open(path)
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        tid=ll[0]
        action=ll[1]
        num=ll[2]
        if(action not in rDict):
            rDict[action]={}
        if(tid not in rDict[action]):
            rDict[action][tid]=num
    fin.close()
    return rDict


if __name__ == '__main__':
    pvDict=initPvDict("pv_dict")
    cataDict=initDict("goods_cata",0,1)
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        tid1=ll[0]
        tid2=ll[1]
        action=ll[2]
        if(action!="like" and action!="cart" and action!="see"):
            continue
        co=int(ll[3])
        if(co<2):
            continue
        cataid1=""
        if(tid1 in cataDict):
            cataid1=cataDict[tid1]
        cataid2=""
        if(tid2 in cataDict):
            cataid2=cataDict[tid2]
        if(cataid1=="" or cataid2=="" or cataid1!=cataid2):
            continue
        pv1=pvDict[action][tid1]
        pv2=pvDict[action][tid2]
        print "%s\t%s\t%s\t%s\t%s\t%s"%(tid1,tid2,co,pv1,pv2,action)
        print "%s\t%s\t%s\t%s\t%s\t%s"%(tid2,tid1,co,pv2,pv1,action)




