#coding=utf8
import sys
import os
import time

def printResult(uid, buf):
    if(len(buf) > 5):
        return
    for i in range(len(buf)):
        for j in range(len(buf)):
            if(i == j):
                continue
            print "cart\t%s\t1\t%s\t%s\t%s\t%s"%(uid, buf[i][0], buf[j][0], "\t".join(buf[i][1:]), "\t".join(buf[j][1:]))

if __name__ == '__main__':
    buf=[]
    uid=""
    usedDict={}
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            printResult(uid, buf)
            buf = []
            usedDict={}
            continue
        try:
            uid, tid, ctime, price, color, size = line.split("\t")
        except:
            continue
        if(tid not in usedDict):
            buf.append([tid, ctime, price, color, size])
            usedDict[tid] = 1
        
        
        

        
         













