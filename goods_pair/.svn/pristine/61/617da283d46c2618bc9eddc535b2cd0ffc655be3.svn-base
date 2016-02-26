#!/usr/bin/python
#coding=utf8
import sys

def printResult(cata1, buf):
    rList = []
    for tpl in buf:
        cata2 = tpl[0]
        co = float(tpl[1])
        pv1 = float(tpl[2])
        pv2 = float(tpl[3])
        if(co < 10):
            continue
        rList.append([cata2, co, pv1, pv2, 1000*co/((pv1*pv1 + pv2*pv2)**0.5)])
    rList.sort(cmp=lambda x,y:cmp(x[4],y[4]), reverse=True)
    rList = rList[0:7]
    for tpl in rList:
        print "%s\t%s\t%s\t%s\t%s\t%s"%(cata1, tpl[0], tpl[1], tpl[2], tpl[3], tpl[4])
        


if __name__ == '__main__':
    key=""
    key_bak=""
    buf = []
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0].strip()
        if(key!=key_bak):
            if(key_bak!="" and len(buf) > 0):
                printResult(key_bak, buf)
            buf = []
            key_bak=key
        
        buf.append(ll[1:])


    if(key_bak!="" and len(buf) > 0):
        printResult(key_bak, buf)
        


