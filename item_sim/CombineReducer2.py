#!/usr/bin/python
#coding=utf8
import sys
import time

def printResult(key,tidList):
    arr=key.split("{/c}")
    action=arr[2]
    for i in range(len(tidList)):
        for j in range(len(tidList)):
            if(i==j):
                continue
            print "%s\t%s\t%s\t%s"%(tidList[i],tidList[j],1,action)


if __name__ == '__main__':
    key=""
    key_bak=""
    tidList=[]
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0]
        if(key!=key_bak):
            if(key_bak!="" and len(tidList)>0):
                printResult(key_bak,tidList)
            key_bak=key
            tidList=[]
        tid=ll[1]
        tidList.append(tid)

    if(key_bak!="" and len(tidList)>0):
        printResult(key_bak,tidList)


