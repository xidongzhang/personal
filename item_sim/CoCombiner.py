#!/usr/bin/python
#coding=utf8
import sys
import time


if __name__ == '__main__':
    key=""
    key_bak=""
    num=0
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0]
        if(key!=key_bak):
            if(key_bak!="" and num>0):
                print "%s\t%s"%(key_bak,num)
            key_bak=key
            num=0
        num+=int(ll[1])

    if(key_bak!="" and num>0):
        print "%s\t%s"%(key_bak,num)


