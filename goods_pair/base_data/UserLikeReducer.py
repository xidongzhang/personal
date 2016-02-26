#!/usr/bin/python
#coding=utf8
import sys

sessionLimit=3600*24*5
def sessionEnd(lastTime, ctime):
    if(lastTime=="" or ctime==""):
        return False
    lastTime=int(lastTime)
    ctime=int(ctime)
    if(ctime-lastTime>sessionLimit):
        return True
    else:
        return False

def printResult(uid, buf):
    if(len(buf)==0):
        return
    for tpl in buf:
        print "%s\t%s\t%s"%(uid,tpl[0],tpl[1])
    print

if __name__ == '__main__':
    key=""
    key_bak=""
    ctime=""
    lastTime=""
    buf=[]
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0].strip()
        if(key!=key_bak):
            if(key_bak!=""):
                printResult(key_bak, buf)
            buf=[]
            ctime=""
            lastTime=""
            key_bak=key
        tid=ll[2]
        ctime=ll[1]
        if(sessionEnd(lastTime, ctime)):
            printResult(key, buf)
            buf=[]
            lastTime=""
        buf.append((tid, ctime))
        lastTime=ctime

    if(key_bak!=""):
        printResult(key_bak, buf) 


