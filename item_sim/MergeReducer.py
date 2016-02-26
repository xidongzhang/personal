#!/usr/bin/python
#coding=utf8
import sys
import time


if __name__ == '__main__':
    key=""
    key_bak=""
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0]
        if(key!=key_bak):
            if(key_bak!=""):
                print "\t".join(key_bak.split("{/c}"))
            key_bak=key
    if(key_bak!=""):
        print "\t".join(key_bak.split("{/c}"))


