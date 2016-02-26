#!/usr/bin/python
#coding=utf8
import sys

sessionLimit=3600*24*14
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
    if(len(buf) < 2 or len(buf) > 5):
        return
    for i in range(len(buf)):
        for j in range(len(buf)):
            if(i == j or buf[i][0] == buf[j][0]):
                continue
            print "buy\t%s\t1\t%s\t%s\t-\t-\t-\t-\t-\t-\t-\t-"%(uid, buf[i][0], buf[j][0])

if __name__ == '__main__':
    key=""
    key_bak=""
    buf=[]
    lastTime=""
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        key, ctime, tid = line.strip().split('\t')
        if(key!=key_bak):
            if(key_bak!=""):
                printResult(key_bak, buf)
            buf=[]
            lastTime=""
            key_bak=key
        
        if(sessionEnd(lastTime, ctime)):    
            printResult(key, buf)  
            buf=[] 
            lastTime=""
        buf.append((tid, ctime)) 
        lastTime=ctime

    if(key_bak!=""):
        printResult(key_bak, buf) 


