#!/usr/bin/python
#coding=utf8
import sys
import time

splitor=sys.argv[1]
def printResult(key,numList):
    arr=[key]
    if(splitor!="NULL"):
        arr=key.split(splitor)
    print "%s\t%s"%("\t".join(arr),"\t".join(map(str,numList)))

if __name__ == '__main__':
    key=""
    key_bak=""
    numList=[]
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0]
        if(key!=key_bak):
            if(key_bak!=""):
                printResult(key_bak,numList)
            key_bak=key
            numList=[]
        if(len(numList)==0):
            for i in range(len(ll)-1):
                numList.append(0)
        for i in range(len(ll)-1):
            numList[i]+=float(ll[i+1])

    if(key_bak!=""):
        printResult(key_bak,numList)


