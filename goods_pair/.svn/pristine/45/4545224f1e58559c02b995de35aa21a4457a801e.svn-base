#!/usr/bin/python
#coding=utf8
import sys

if __name__ == '__main__':
    key=""
    key_bak=""
    buf = {}
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll = line.strip().split('\t')
        key = ll[0]
        if(key!=key_bak):
            if(key_bak!="" and len(buf) > 0):
                pList = []
                for t in buf:
                    pList.append(t)
                print "%s\t%s\t2"%(key_bak.replace("{/c}","\t"), ",".join(pList))
            buf = {}
            key_bak=key
        tid = ll[1]
        buf[tid] = 1

    if(key_bak!="" and len(buf) > 0):
        pList = []
        for t in buf:
            pList.append(t)
        print "%s\t%s\t2"%(key_bak.replace("{/c}","\t"), ",".join(pList))


