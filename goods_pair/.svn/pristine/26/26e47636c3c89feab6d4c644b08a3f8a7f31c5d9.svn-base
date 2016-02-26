#coding=utf8
import sys
import os
import time
import random
from utils import *

#\001    \t
#\002    ,
#\003    :
            
def checkTime(ctime,curtime,dayRange):
    if((curtime-int(ctime)) > dayRange*24*3600):
        return False
    else:
        return True

def getValue(sStr, key):
    arr=sStr.split("\002")
    v=None
    for tStr in arr:
        tArr=tStr.split("\003")
        if(len(tArr)==2 and tArr[0]==key):
            v=tArr[1]
            break
    return v

def checkRand(num):
    r=random.randint(1,100)
    if(r<=num):
        return True
    else:
        return False

if __name__ == '__main__':
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        print line

