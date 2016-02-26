#!/usr/bin/python
#coding=utf8
import sys

if __name__ == '__main__':
    key=""
    key_bak=""
    numList=[]
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0].strip()
        if(key!=key_bak):
            if(key_bak!=""):
                print "%s\t%s"%(key_bak,"\t".join(map(str,numList)))
            numList=[]
            key_bak=key
        if(len(numList)==0):
            for i in range(len(ll)-1):
                numList.append(0)
        for i in range(len(ll)-1):
            numList[i]+=float(ll[i+1])


    if(key_bak!=""):
        print "%s\t%s"%(key_bak,"\t".join(map(str,numList)))
        


