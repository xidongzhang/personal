#!/usr/bin/python
#coding=utf8
import sys

sign = sys.argv[1]
def printResult(buf1, buf2):
    for i in range(len(buf1)):
        for j in range(len(buf2)):
            tpl1 = buf1[i]
            tpl2 = buf2[j]
            tid1 = tpl1[0]
            tid2 = tpl2[0]
            print "%s\t%s\t%s\t-\t-\t1\t%s\t%s"%(tid1, tid2, sign, "\t".join(tpl1[1]), "\t".join(tpl2[1]))

if __name__ == '__main__':
    key = ""
    key_bak = ""
    buf1 = []
    buf2 = []
    for line in sys.stdin:
        if(line.strip() == ''):
            continue
        ll = line.strip().split('\t')
        key = ll[0]
        if(key != key_bak):
            if(key_bak != ""):
                printResult(buf1, buf2)
            buf1 = []
            buf2 = []
            key_bak = key

        arr = ll[1].split("{/c}")
        tid = arr[0]
        id = arr[1]
        feaList = ll[2].split("_::_")
        if(id == "1"):
            buf1.append((tid, feaList))
        elif(id == "2"):
            buf2.append((tid, feaList))

    if(key_bak != ""):
        printResult(buf1, buf2)



