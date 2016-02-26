#coding=utf8
import sys
import os

def initPvDict(path):
    rDict={}
    fin=open(path)
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        tid=ll[0]
        action=ll[1]
        num=ll[2]
        if(action not in rDict):
            rDict[action]={}
        if(tid not in rDict[action]):
            rDict[action][tid]=num
    fin.close()
    return rDict

if __name__ == '__main__':
    pvDict=initPvDict("pv_dict")
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        uid=ll[0]
        tid=ll[1]
        date=ll[2]
        action=ll[3]
        if(action not in pvDict or tid not in pvDict[action]):
            continue
        print "%s{/c}%s{/c}%s\t%s"%(uid,date,action,tid)



