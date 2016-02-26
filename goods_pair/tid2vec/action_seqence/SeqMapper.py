#coding=utf8
import sys
import os
import time
from utils import *


if __name__ == '__main__':
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            continue
        ll = line.split("\t")
        if(len(ll) != 5):
            continue
        timeStr = ll[0]
        uid = ll[1]
        if(uid == "0" or uid == ""):
            continue
        action = ll[3]
        if(action == "search"):
            continue
        tid = ll[4]
        ctime = getTimeStamp(timeStr)
        print "%s\t%s\t%s"%(uid, ctime, tid)





