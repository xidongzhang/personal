#!/usr/bin/python
#coding=utf8
import sys
from utils import *

ctrDict = initDict("click_rate_dict", 0, 1)
def printResult(key, buf):
    buf.sort(cmp=lambda x,y:cmp(x[1],y[1]),reverse=True)
    pList = []
    for tpl in buf:
        pList.append(tpl[0])
    print "%s\t%s"%(key_bak.replace("{/c}","\t"), ",".join(pList))

if __name__ == '__main__':
    key=""
    key_bak=""
    buf = []
    defaultCtr = sys.argv[1]
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll = line.strip().split('\t')
        key = ll[0]
        if(key!=key_bak):
            if(key_bak!=""):
                printResult(key_bak, buf)
            buf = []
            key_bak=key
        tid = ll[1]
        score = defaultCtr
        if(tid in ctrDict):
            score = ctrDict[tid]
        buf.append((tid, float(score)))

    if(key_bak!=""):
        printResult(key_bak, buf)


