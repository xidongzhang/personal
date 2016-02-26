#!/usr/bin/python
#coding=utf8
import sys

def printResult(order_id,ctime,vList):
    for v in vList:
        print "%s\t%s\t%s"%(order_id,ctime,v)

if __name__ == '__main__':
    key=""
    key_bak=""
    ctime=""
    vList=[]
    black=False
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0].strip()
        if(key!=key_bak):
            if(key_bak!="" and ctime!="" and not black):
                printResult(key_bak,ctime,vList)
            ctime=""
            vList=[]
            black=False
            key_bak=key
        if(len(ll)==2):
            ctime=ll[1]
        elif(len(ll)==1):
            black=True
        else:
            tid=ll[1]
            uid=ll[2]
            price=ll[3]
            color=ll[4]
            size=ll[5]
            v="\t".join(ll[1:])
            vList.append(v)

    if(key_bak!="" and ctime!="" and not black):
        printResult(key_bak,ctime,vList)
        


