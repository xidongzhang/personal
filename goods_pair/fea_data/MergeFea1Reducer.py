#!/usr/bin/python
#coding=utf8
import sys

if __name__ == '__main__':
    key = ""
    key_bak = ""
    feaArr = []
    for line in sys.stdin:
        if(line.strip() == ''):
            continue
        ll = line.strip().split('\t')
        key = ll[0]
        if(key!=key_bak):
            feaArr = []
            key_bak=key
        
        sign = ll[1]
        if(sign == "0"):
            feaArr = ll[2:]
        else:
            if(len(feaArr) > 0):
                print "%s\t%s"%("\t".join(ll[2:]), "\t".join(feaArr))



