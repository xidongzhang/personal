#coding=utf8
import sys
import os
import time
from utils import *

if __name__ == '__main__':
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        #print >> sys.stderr, line
        timeStr, uid, action, twitter_id, query, twitter_pos=line.split("\001")
        if(uid == "0"):
            continue
        timeStr=getTimeStamp(timeStr)
        print "%s\t%s\t%s\t%s\t%s\t%s"%(uid, timeStr, action, twitter_id, query, twitter_pos)
        

        
         













