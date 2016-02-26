#!/usr/bin/python
#coding=utf8
import sys

def printResult(buf):
    for i in range(len(buf)):
        for j in range(i+1, len(buf)):
            tid1 = buf[i][0]
            tid2 = buf[j][0]
            print "%s\t%s\tno\t-\t-\t1\t%s\t%s"%(tid1, tid2, "\t".join(buf[i][1:]), "\t".join(buf[j][1:]))


if __name__ == '__main__':
    key = ""
    key_bak = ""
    buf = []
    for line in sys.stdin:
        if(line.strip() == ''):
            continue
        ll = line.strip().split('\t')
        key = ll[0]
        if(key != key_bak):
            if(key_bak != ""):
                printResult(buf)
            buf = []
            key_bak = key

        buf.append(ll[1:])

    if(key_bak != ""):
        printResult(buf)



