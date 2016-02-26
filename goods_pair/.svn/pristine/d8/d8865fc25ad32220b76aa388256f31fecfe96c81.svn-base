#coding=utf8
import sys
import urllib
import time

def calCos(dict1,dict2):
    sum=0.0
    for k1 in dict1:
        if(k1 in dict2):
            sum+=dict1[k1]*dict2[k1]
    squSum1=0.0
    for k1 in dict1:
        squSum1+=(dict1[k1]*dict1[k1])
    squSum2=0.0
    for k2 in dict2:
        squSum2+=(dict2[k2]*dict2[k2])
    sim=-1
    if(squSum2>0 and squSum1>0):
        sim=sum/((squSum1*squSum2)**0.5)
    return sim

def getTimeStr(timeStamp=time.time(),format="%Y-%m-%d %H:%M:%S"):
    ltime=time.localtime(float(timeStamp))
    timeStr=time.strftime(format, ltime)
    return timeStr

def getTimeStamp(timeStr,formatStr="%Y-%m-%d %H:%M:%S"):
    timeStamp=0
    #try:
    timeArray = time.strptime(timeStr,formatStr)
    timeStamp = int(time.mktime(timeArray)) 
    #except:
    #    timeStamp=-1
    return timeStamp

def getTimeScore(curTime,ctime):
    minScore=0.4
    minTime=curTime-3600*24*60 
    diff=curTime-ctime
    score=1-diff*(1-minScore)/(curTime-minTime) 
    if(score<0):
        score=0
    return score

def initDict(path,kIndex,vIndex,separator="\t"):
    fin=open(path)
    rDict={}
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        line=line.lower()
        if(line.startswith("#")):
            continue
        ll=line.split(separator)
        key=ll[kIndex]
        value=1
        if(vIndex>=0):
            value=ll[vIndex]
        rDict[key]=value
    fin.close()
    return rDict

def parseLine(line,metaList,separator="\t"):
    ll=line.split(separator)
    vDict={}
    if(len(ll)!=len(metaList)):
        return vDict
    for i in range(len(ll)):
        vDict[metaList[i]]=ll[i]
    return vDict

def initList(path,index,separator="\t"):
    fin=open(path)
    rList=[]
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        line=line.lower()
        if(line.startswith("#")):
            continue
        ll=line.split(separator)
        v=ll[index]
        rList.append(v)
    fin.close()
    return rList

def decode(v):
    try:
        vStr=urllib.unquote(v)
        #vStr=vStr.decode("utf8")
        #vStr=vStr.encode("gb18030")
        return vStr
    except:
        return v

def addToPrint(key,v,vDict,limit):
    if(key in vDict):
        vDict[key]+=v
    else:
        vDict[key]=v
        if(len(vDict)>=limit):
            for k in vDict:
                print "%s\t%s"%(k,str(vDict[k]))
            vDict.clear()

def printDict(vDict):
    for k in vDict:
        print "%s\t%s"%(k,str(vDict[k]))

def filtStr(vStr,filtList,replaceDict,splitList,isRetList):
    res=""
    if(isRetList):
        res=[]
        for splitor in splitList:
            if(vStr.find(splitor)>=0):
                res=vStr.split(splitor)
                break
    if(isRetList):
        if(len(res)==0):
            res.append(vStr)
        retList=[]
        for v in res:
            retList.append(filtStr(v,filtList,replaceDict,[],False))
        return retList
    else:
        try:
            for s in filtList:
                vStr=vStr.replace(s,"")
            for s in replaceDict:
                vStr=vStr.replace(s,replaceDict[s])
        except:
            pass
        return vStr

def toLower(vStr):
    try:
        s=unicode(vStr,"gb18030")
        retStr=""
        for i in range(len(s)):
            v=s[i].encode("gb18030")
            if(v.isalpha()):
                v=v.lower()
                retStr+=v
    except:
        return vStr
    return retStr

def removeBracket(vStr):
    retStr=vStr
    pos=vStr.find("(")
    if(pos>0):
        retStr=vStr[0:pos]
    pos=vStr.find("（")
    if(pos>0):
        retStr=vStr[0:pos]
    return retStr

def getHash(value):
    h = 0;  
    if (h == 0 and len(value) > 0):
        for i in range(len(value)):
            h = 31 * h + ord(value[i])
        hash = h 
    return h


if __name__ == '__main__':
    #print decode("%E7%A7%8B%E5%AD%A3%E6%96%B0%E6%AC%BE")
    #print decode("%E8%BF%90%E5%8A%A8%E9%9E%8B")
    '''
    a="厚薄：："
    print filtStr(a,[":","："],{"\t":" "},[],False)    
    print a

    s="马伊琍"
    print s.lower()
    print toLower(s)

    v="黑色 黄色 草绿 红色 玫红 白色 果绿 灰色 蓝色"
    v=filtStr(v,[],{"\t":" "},[","," "," "],True)
    print v

    ss1="裙长_短裙（76-90cm）"
    ss2="裙长_短裙(76-90cm)"
    print removeBracket(ss1)         
    print removeBracket(ss2)         
    '''
    print getHash("aabcdasf`:wq")
    
    
