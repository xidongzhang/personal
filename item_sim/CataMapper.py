#coding=utf8
import sys
import os

if __name__ == '__main__':
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        tid=ll[1]
        cataStr=ll[12]
        status=ll[8]
        if(status!="1"):
            continue
        arr=cataStr.split("{/c}")
        if(len(arr)!=3):
            continue
        pCataStr=arr[2]
        if(pCataStr!="-"):
            pCataStr=arr[1]
        else:
            pCataStr=arr[0]
        cataid=pCataStr[0:pCataStr.find(":")]
        print "%s\t%s"%(tid,cataid)



