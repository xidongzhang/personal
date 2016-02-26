#coding=utf8
import sys
import os
import time
from utils import *
            
def checkTime(ctime,curtime,dayRange):
    if((curtime-int(ctime)) > dayRange*24*3600):
        return False
    else:
        return True

if __name__ == '__main__':

    input_file = os.environ['map_input_file']
    arr=input_file.split("/")
    curtime=time.time()
    for line in sys.stdin:
        line=line.strip()
        if(line==""):
            continue
        if(arr[len(arr)-2]=="order_filt"):
            ll=line.split("\t")
            ctime=ll[1].strip()
            tid=ll[2].strip()
            uid=ll[3].strip()
            price=ll[4]
            color=ll[5]
            size=ll[6]
            if(uid=="null" or uid==""):
                continue
            timeStr=getTimeStr(ctime,"%Y-%m")
            if(checkTime(ctime,curtime,365)):
                print "%s{/c}%s{/c}%s{/c}buy"%(uid,tid,timeStr)
        elif(arr[len(arr)-2]=="user_like"):
            ll=line.split("\t")
            uid=ll[0].strip()
            tid=ll[1].strip()
            ctime=ll[2].strip()
            if(uid=="null" or uid==""):
                continue
            timeStr=getTimeStr(ctime,"%Y-%m")
            if(checkTime(ctime,curtime,365)):
                print "%s{/c}%s{/c}%s{/c}like"%(uid,tid,timeStr)
        elif(arr[len(arr)-4]=="logs"):
            ll=line.split("\t")
            uid=ll[0].strip()
            if(uid=="null" or uid==""):
                continue
            pageType=ll[2]
            ctime=ll[1].strip()
            if(ctime.endswith(".0")):
                ctime=ctime[0:len(ctime)-2]
            timeStamp =0
            try:
                timeArray = time.strptime(ctime, "%Y-%m-%d %H:%M:%S")
                timeStamp = int(time.mktime(timeArray))
            except:
                continue
            if(pageType=="twitter"):
                tid=ll[3].strip()
                if(tid=="-"):
                    continue
                refer=ll[4].strip()
                if(checkTime(timeStamp,curtime,60)):
                    timeStr=getTimeStr(timeStamp,"%Y-%m-%d")
                    print "%s{/c}%s{/c}%s{/c}see"%(uid,tid,timeStr)
        elif(arr[len(arr)-3]=="ods_bat_shopcart"):
            ll=line.split("\001")
            uid=ll[1].strip()
            tid=ll[3].strip()
            price=ll[4].strip()
            color=ll[6].strip()
            size=ll[7].strip()
            ctime=ll[9].strip()
            if(uid=="null" or uid==""):
                continue
            timeStr=getTimeStr(ctime,"%Y-%m")
            if(checkTime(ctime,curtime,365)):
                print "%s{/c}%s{/c}%s{/c}cart"%(uid,tid,timeStr)


        
         













