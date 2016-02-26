#!/usr/bin/python
#coding=utf8
import sys

def printResult(order_id, buf):
    if(len(buf) < 2 or len(buf) > 3):
        return
    for i in range(len(buf)):
        for j in range(len(buf)):
            if(i == j or buf[i][0] == buf[j][0]):
                continue
            print "same_order\t%s\t1\t%s\t%s\t%s\t%s"%(order_id, buf[i][0], buf[j][0], "\t".join(buf[i][1:]), "\t".join(buf[j][1:]))

if __name__ == '__main__':
    key=""
    key_bak=""
    buf=[]
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        key, ctime, tid, uid, price, color, size = line.strip().split('\t')
        if(key!=key_bak):
            if(key_bak!=""):
                printResult(key_bak, buf)
            buf=[]
            key_bak=key
        
        buf.append([tid, ctime, price, color, size])

    if(key_bak!=""):
        printResult(key_bak, buf) 


