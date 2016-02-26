#!/usr/bin/python
#coding=utf8
import sys

def printResult(key_bak,numList,seperator):
    arr=key_bak.split(seperator)
    print "%s\t%s"%("\t".join(map(str,numList)), "\t".join(arr))

if __name__ == '__main__':
    key = ""
    key_bak = ""
    seperator = "_::_"
    numList = []
    for line in sys.stdin:
        if(line.strip() == ''):
            continue
        ll = line.strip().split('\t')
        key = ll[0]
        if(key != key_bak):
            if(key_bak != ""):
                printResult(key_bak,numList,seperator)
            numList = []
            key_bak = key
        if(len(numList)==0):
            for i in range(len(ll)-1):
                numList.append(0)
        for i in range(len(ll)-1):
            numList[i]+=float(ll[i+1])

    if(key_bak != ""):
        printResult(key_bak,numList,seperator)



