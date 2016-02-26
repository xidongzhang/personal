#coding=utf8
import sys
import os
import time
import random
from utils import *

#\001    \t
#\002    ,
#\003    :
            
def checkTime(ctime,curtime,dayRange,dayLast):
    if((curtime-int(ctime)) > dayRange*24*3600 or (curtime-int(ctime)) < dayLast*24*3600):
        return False
    else:
        return True

def getValue(sStr, key):
    arr=sStr.split("\002")
    v=None
    for tStr in arr:
        tArr=tStr.split("\003")
        if(len(tArr)==2 and tArr[0]==key):
            v=tArr[1]
            break
    return v

def checkRand(num):
    r=random.randint(1,100)
    if(r<=num):
        return True
    else:
        return False

if __name__ == '__main__':
    curtime=time.time()
    input_file = os.environ['map_input_file']
    arr=input_file.split("/")
    dateStr=arr[len(arr)-2]
#    dateStr=sys.argv[2]
    dateStr=dateStr[dateStr.find("=")+1:]
    ctime=getTimeStamp(dateStr,"%Y-%m-%d") 
    dayLast=int(sys.argv[2])
    #maxTime=getTimeStamp(dayLast,"%Y-%m-%d")
    dayLimit=float(sys.argv[1])
    sample=int(sys.argv[3])
    #breakSign=False
    #if(not checkTime(ctime,curtime,dayLimit,dayLast)):
    #    breakSign=True
    actionDict=initDict("action_meta",0,1)
    #dayLimit=60
    isPersonal=sys.argv[4]
    sign=sys.argv[5]
    for line in sys.stdin:
    #    if(breakSign):
    #        continue
        line=line.strip()
        if(line==""):
            continue
        ll=line.split("\001")
        #print "\t".join(ll)
        #continue
        timeStr="%s %s:%s:%s"%(ll[0],ll[1],ll[2],ll[3])
        #ctime=getTimeStamp(timeStr) 
        #if(not checkTime(ctime,curtime,dayLimit)):
        #    continue
        mobOs=ll[4]
        uid=ll[8]
        ip=ll[9]
        actionid=ll[10]
        tid_scenario=ll[12]
        tid=getValue(tid_scenario,"twitter_id")
        if(tid==None):
            continue
        if(sign=="train" and uid=="0"):
            continue
        if(uid=="0"):
            r=random.randint(1,5000)
            uid="uid_%s"%(r)
        list_scenario=ll[13]
        offset=getValue(list_scenario,"offset")
        pos=getValue(list_scenario,"pos")
        query=getValue(list_scenario,"query")
        if(offset==None or pos==None or query==None):
            continue
        realPos=-1
        try:
            realPos=int(pos)+int(offset)
        except:
            continue
        if(realPos<=0):
            continue
        resArr=[0,0]
        if(actionid=="search-tag_poster"):
            if(not checkRand(sample)):
                continue
            resArr[0]+=1
        elif(actionid=="statistics-single_twitter"):
            resArr[1]+=1
        else:
            continue
        if(isPersonal=="0"):
            uid="a"
        outKey="%s{/c}%s{/c}%s{/c}%s"%(uid,tid,query,realPos) 
        print "%s\t%s"%(outKey,"\t".join(map(str,resArr)))


