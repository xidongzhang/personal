#coding=utf8
import sys
import os
import time

def initDict(path):
    addiDict={}
    fin=open(path)
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\t")
        addiDict[ll[0]]=1
    fin.close()
    return addiDict

def checkUsed(start,length,posUsedDict):
    for i in range(start,start+length):
        if(i in posUsedDict):
            return False
    return True

def wordSplit(wordList,addiDict):
    resultArr=[]
    posUsedDict={}
    for length in range(4,0,-1):
        if(len(wordList)<length):
            continue
        for i in range(len(wordList)-length+1):
            sign=checkUsed(i,length,posUsedDict)
            if(not sign):
                continue
            if(length==1):
                resultArr.append(wordList[i])
                posUsedDict[i]=1
            else:
                wordStr="".join(wordList[i:i+length])
                if(wordStr in addiDict):
                    resultArr.append(wordStr)
                    for j in range(i,i+length):
                        posUsedDict[j]=1
    return resultArr
            


if __name__ == '__main__':

    input_file = os.environ['map_input_file']
    arr=input_file.split("/")
    inputPath=arr[len(arr)-3]
    #inputPath="ods_bat_order"
    #inputPath="ods_bat_goods_map"
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        #ll=line.split("\t")
        ll=line.split("\001")
        if(inputPath=="ods_bat_order"):
            order_id=ll[1]
            #status=int(ll[3])
            ctime=ll[5]
            paytime=int(ll[15].strip())
            #if(status>=20 and status<50):
            if(paytime>0):
                print "%s\t%s"%(order_id,ctime)
        elif(inputPath=="ods_bat_goods_map"):
            order_id=ll[1]
            tid=ll[2].strip()
            uid=ll[20].strip()
            price=ll[7].strip()
            if(price=="null" or price==""):
                price="-"
            color=ll[9].strip()
            if(color=="null" or color==""):
                color="-"
            color=color.replace("\t"," ")
            color=color.replace("\n"," ")
            size=ll[10].strip()
            if(size=="null" or size==""):
                size="-"
            if(uid=="null" or uid==""):
                continue
            size=size.replace("\t"," ")
            size=size.replace("\n"," ")
            print "%s\t%s\t%s\t%s\t%s\t%s"%(order_id,tid,uid,price,color,size)
        elif(inputPath=="ods_focus_fake_orders"):
            order_id=ll[1]
            isFake=ll[12]
            if(isFake=="1"):
                print order_id

        
         













