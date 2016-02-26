#coding=utf8
import sys
import os
import time

if __name__ == '__main__':
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        try:
            uid, ctime, action, tid, query, pos = line.split("\t")
        except:
            continue
        if(action != "order-pay"):
            continue
        print "%s\t%s\t%s"%(uid, ctime, tid)
        
        
        

        
         













