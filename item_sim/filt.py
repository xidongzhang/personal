import sys
from utils import *

def filtDict(path,brdDict):
    fin=open(path)
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        tid=ll[0]
        #if(tid not in brdDict or int(tid)<3003992093):
        if(tid not in brdDict):
            continue
        action = ll[1]
        num=int(ll[2])
        #if(action == "see" and num < 6):
        #    continue
        if(num<5):
            continue
        print line
    fin.close()

if '__main__' == __name__:
    brdDict=initDict("brd_goods",0,-1)
    filtDict("goods_pv",brdDict)


    #brdDict=initDict("format_co_final",0,1)
    #print "finish"
    #time.sleep(30)



