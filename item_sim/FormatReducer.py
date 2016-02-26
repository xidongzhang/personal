#!/usr/bin/python
#coding=utf8
import sys
import time

def printResult(tid1,bakDict):
    sList=[]
    for tid2 in bakDict:
        if(bakDict[tid2][0]>=3):
            cosCo=bakDict[tid2][0]/((bakDict[tid2][1]*bakDict[tid2][2])**0.5)*10
            sList.append((tid2,cosCo))
        elif(bakDict[tid2][3]>=5):
            cosCo=bakDict[tid2][3]/((bakDict[tid2][4]*bakDict[tid2][5])**0.5)
            sList.append((tid2,cosCo))
    if(len(sList)<4):
        return
    sList.sort(cmp=lambda x,y:cmp(x[1],y[1]),reverse=True)
    sList=sList[0:30]
    pList=[]
    for tuple in sList:
        pList.append("%s:%s"%(tuple[0],round(tuple[1],3)))
    print "%s\t%s"%(tid1,",".join(pList))


'''
def printResult(tid1,bakDict):
    sList=[]
    for tid2 in bakDict:
        if(bakDict[tid2][0]<3):
            continue
        cosCo=bakDict[tid2][0]/((bakDict[tid2][1]*bakDict[tid2][2])**0.5)
        sList.append((tid2,cosCo))
    if(len(sList)<3):
        return
    sList.sort(cmp=lambda x,y:cmp(x[1],y[1]),reverse=True)
    sList=sList[0:20]
    pList=[]
    for tuple in sList:
        pList.append("%s:%s"%(tuple[0],round(tuple[1],3)))
        #print "%s\t%s\t%s\t%s"%(tid1,tid2,"\t".join(map(str,bakDict[tid2])),tuple[1])
    print "%s\t%s"%(tid1,",".join(pList))
'''


if __name__ == '__main__':
    key=""
    key_bak=""
    bakDict={}
    for line in sys.stdin:
        if(line.strip()==''):
            continue
        ll=line.strip().split('\t')
        key=ll[0]
        if(key!=key_bak):
            if(key_bak!="" and len(bakDict)>0):
                printResult(key_bak,bakDict)
            key_bak=key
            bakDict={}
        tid2=ll[1]
        co=float(ll[2])
        pv1=float(ll[3])
        pv2=float(ll[4])
        action=ll[5]
        if(tid2 not in bakDict):
            bakDict[tid2]=[0.0,0.0,0.0, 0.0,0.0,0.0]    #co,pv1,pv2,seeCo,seePv1,seePv2
        if(action=="see"):
            bakDict[tid2][3]+=co
            bakDict[tid2][4]+=pv1
            bakDict[tid2][5]+=pv2
        else:
            bakDict[tid2][0]+=co
            bakDict[tid2][1]+=pv1
            bakDict[tid2][2]+=pv2

    if(key_bak!="" and len(bakDict)>0):
        printResult(key_bak,bakDict)





