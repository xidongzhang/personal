#!/usr/bin/python
#coding=utf8
import sys
import time
from utils import *

def printResult(uid, buf):
    if(len(buf) < 8):
        return
    print " ".join(buf)

if __name__ == '__main__':
    key=""
    key_bak=""
    buf = []
    for line in sys.stdin:
        line = line.strip()
        if(line == ''):
            continue
        ll=line.split('\t')
        key=ll[0]
        if(key != key_bak):
            if(key_bak != ""):
                printResult(key_bak, buf)
            buf = []
            key_bak=key
        tid = ll[2]
        buf.append(tid)

    if(key_bak != ""):
        printResult(key_bak, buf)



