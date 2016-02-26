#!/usr/bin/python
#coding=utf8
import sys
import time

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
        print "%s\t%s"%(uid,"\t".join(tpl))
    print

if __name__ == '__main__':
    key=""
    key_bak=""
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
            key_bak=key
            buf=[]
            lastTime=""
        ctime=ll[1]
        tid=ll[2]
        color=ll[3]
        size=ll[4]
        price=ll[5]
        if(sessionEnd(lastTime, ctime)):
            printResult(key, buf)   
            buf=[]  
            lastTime=""
        buf.append([tid, ctime, price, color, size]) 
        lastTime=ctime

    if(key_bak!=""):
        printResult(key_bak, buf)
        


