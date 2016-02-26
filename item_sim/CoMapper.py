#coding=utf8
import sys
import os

resDict={}
limit=100000
validDic={"see":100,"like":100,"buy":30,"cart":100}
def printResult():
    global resDict
    for key in resDict:
        print "%s\t%s"%(key,resDict[key])
    resDict={}

def addResult(key,value):
    global resDict
    if(key not in resDict):
        resDict[key]=0
    resDict[key]+=1
    if(len(resDict)>limit):
        printResult()

def process(key,tidList):
    if(len(tidList)<1):
        return
    arr=key.split("\t")
    action=arr[1]
    if(len(tidList)>validDic[action]):
        return
    for i in range(len(tidList)):
        for j in range(i+1,len(tidList)):
            tid1=tidList[i]
            tid2=tidList[j]
            if(int(tid1)>int(tid2)):
                tid1=tidList[j]
                tid2=tidList[i]
            k="%s{/c}%s{/c}%s"%(tid1,tid2,action)
            print "%s\t%s"%(k,1)
#            addResult(k,1)

if __name__ == '__main__':
    key=""
    key_bak=""
    tidList=[]
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        arr=ll[0].split("{/c}")
        uid=ll[1]
        tid=ll[2]
        date=arr[0]
        action=arr[1]
        key="%s\t%s\t%s"%(date,action,uid)
        if(key!=key_bak):
            if(key_bak!=""):
                process(key_bak,tidList)
            tidList=[]
            key_bak=key
        tidList.append(tid)
    if(key_bak!=""):
        process(key_bak,tidList)
    #printResult()
