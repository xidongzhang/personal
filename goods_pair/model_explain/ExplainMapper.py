#coding=utf8
import sys
import os
import time
#from utils import *

if __name__ == '__main__':
    #feaDict=initDict("fea_index",1,0)
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        if(len(ll)==4):
            fea=ll[0]
            index=int(ll[2])
            cnt=ll[1]
            #if(index in feaDict):
            #    print "%s\t%s\t%s"%(fea,feaDict[index],cnt)
            print "%s\t%s\t%s"%(index,fea,cnt)
        else:
            w=ll[0]
            index=ll[1]
            print "%s\t%s"%(index,w)





