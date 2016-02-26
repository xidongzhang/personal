#coding=utf8
import sys
import os
import time
from utils import *

def printResult(uid, buf, weight):
    if(len(buf) < 2 or len(buf) > 30):
        return
    for i in range(len(buf) - 1):
        print "cart\t%s\t%s\t%s\t%s\t%s\t%s"%(uid, weight, buf[i][0], buf[len(buf)-1][0], "\t".join(buf[i][1:]), "\t".join(buf[len(buf)-1][1:]))

actDict = {"statistics-click_buy_button_2-cart":1, "statistics-click_buy_button_2-buy":1, "order-add":2, "order-pay":5}
if __name__ == '__main__':
    buf = []
    uid = ""
    usedDict = {}
    for line in sys.stdin:
        line = line.strip()
        if(line == ""):
            #printResult(uid, buf)
            buf = []
            usedDict={}
            continue
        try:
            uid, ctime, action, tid, query, pos = line.split("\t")
        except:
            continue
        if(tid not in usedDict):
            buf.append([tid, ctime, "-", "-", "-"])
            usedDict[tid] = 1
        if(action in actDict):
            printResult(uid, buf, actDict[action])
            buf = []
            usedDict = {}
        
        
        

        
         













