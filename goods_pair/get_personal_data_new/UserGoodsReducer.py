#!/usr/bin/python
#coding=utf8
import sys
import time
from utils import *
import hashlib

def get_sha1(src):
    return hashlib.sha1(src).hexdigest()

def printResult(goodsInfo,actll):
    uidSign=actll[0]
    uid=uidSign
    if(uidSign.startswith("uid_")):
        uid="0"
    query=actll[1]
    pos=actll[2]
    show=actll[3]
    click=actll[4]
    actArr=[query,pos,uid]
    vStr="%s\t%s"%("\t".join(goodsInfo),"\t".join(actArr))
    #print "%s\t%s\t%s\t%s\t%s"%(uid,show,click,get_sha1(vStr),vStr)
    print "%s\t%s\t%s\t%s"%(uidSign,show,click,vStr)


if __name__ == '__main__':
    key=""
    key_bak=""
    goodsInfo=""
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0]
        if(key!=key_bak):
            key_bak=key
            goodsInfo=""
        sign=ll[1]
        if(sign=="1"):
            goodsInfo=ll[2:]
            goodsInfo.insert(0,key)
        elif(sign=="2"):
            if(goodsInfo==""):
                continue
            printResult(goodsInfo,ll[2:])

        

