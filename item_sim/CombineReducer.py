#!/usr/bin/python
#coding=utf8
import sys
import time

resDict={}

def printResult(resDict):
    for key in resDict:
        print "%s\t%s"%(key,resDict[key])
    resDict={}

def processResult(key,tidList):
    arr=key.split("{/c}")
    action=arr[2]
    for i in range(len(tidList)):
        for j in range(i,len(tidList)):
            tid1=tidList[i]
            tid2=tidList[j]
            k=""
            if(int(tid1)<int(tid2)):
                k="%s\t%s\t%s"%(tid1,tid2,action)
            else:
                k="%s\t%s\t%s"%(tid2,tid1,action)
            if(k not in resDict):
                resDict[k]=0
            resDict[k]+=1
    if(len(resDict)>100000):
        printResult(resDict)


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
                processResult(key_bak,tidList)
            key_bak=key
            tidList=[]
        tid=ll[1]
        tidList.append(tid)

    if(key_bak!="" and len(tidList)>0):
        processResult(key_bak,tidList)
        printResult(resDict)


