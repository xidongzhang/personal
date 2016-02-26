#!/usr/bin/python
#coding=utf8
import sys
import time

if __name__ == '__main__':
    key=""
    key_bak=""
    fea=""
    cnt=""
    w=""
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0]
        if(key!=key_bak):
            if(key_bak!="" and fea!="" and cnt!="" and w!=""):
                if(not fea.startswith("pos&")):
                    print "%s\t%s\t%s"%(fea,w,cnt)
            key_bak=key
            fea=""
            cnt=""
            w=""
        if(len(ll)==3):
            fea=ll[1]
            cnt=int(float(ll[2]))
        elif(len(ll)==2):
            w=ll[1]
            if(key=="1"):
                fea="Beta&0"
                cnt="9999999"

    if(key_bak!="" and fea!="" and cnt!="" and w!=""):
        if(not fea.startswith("pos&")):
            print "%s\t%s\t%s"%(fea,w,cnt)



