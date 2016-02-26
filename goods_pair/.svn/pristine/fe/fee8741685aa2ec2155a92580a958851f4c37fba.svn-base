#coding=utf8
import sys
import os
import time


if __name__ == '__main__':

    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\001")
        uid=ll[1].strip()
        tid=ll[3].strip()
        price=ll[4].strip()
        color=ll[6].strip()
        color=color.replace("\t"," ")
        color=color.replace("\n"," ")
        size=ll[7].strip()
        size=size.replace("\t"," ")
        size=size.replace("\n"," ")
        ctime=ll[9].strip()
        if(uid=="null" or uid==""):
            continue
        print "%s\t%s\t%s\t%s\t%s\t%s"%(uid,ctime,tid,color,size,price)


        
         













