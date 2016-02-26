#coding=utf8
import sys
import os
import time

if __name__ == '__main__':
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        uid=ll[0]
        tid=ll[1]
        ctime=ll[2]
        print "%s\t%s\t%s"%(uid,ctime,tid)
        

        
         













