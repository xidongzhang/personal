#!/usr/bin/python
#coding=utf8
import sys

if __name__ == '__main__':
    key=""
    key_bak=""
    sum = 0
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0].strip()
        if(key!=key_bak):
            if(key_bak!=""):
                print "%s\t%s"%(key_bak, sum)
            sum = 0
            key_bak=key
        
        sum += float(ll[1])


    if(key_bak!=""):
        print "%s\t%s"%(key_bak, sum)
        


