import datetime
import time
import os

def getDateList(startDayStr,endDayStr):
    dList=[]
    endDay=int(endDayStr)
    startTm = time.strptime(startDayStr,"%Y%m%d")
    dateTime=datetime.datetime(startTm.tm_year,startTm.tm_mon,startTm.tm_mday)

    delta=0
    while True:
        day = dateTime + datetime.timedelta(days = delta)
        dayInt=int(day.strftime('%Y%m%d'))
        if(dayInt>endDay):
            break
        dayFormat=day.strftime('%Y-%m-%d')
        dList.append(str(dayFormat))
        delta+=1
    return dList

def getDataFromHDFS(hdfs_path, local_path):
    os.popen('rm -f %s' % (local_path))
    os.popen('hadoop fs -getmerge %s %s' % (hdfs_path, local_path))

def initDict(path,kIndex,vIndex,separator="\t"):
    fin=open(path)
    rDict={}
    for line in fin:
        line=line.strip()
        if(line==""):
            continue
        line=line.lower()
        ll=line.split(separator)
        key=ll[kIndex]
        value=1
        if(vIndex>=0):
            value=ll[vIndex]
        rDict[key]=value
    fin.close()
    return rDict

def sendMail(title, content, reciver):
    sys_cmd = '''echo "%s" | mail -s '%s' '%s' ''' % (content, title, ' '.join([i + '@meilishuo.com' for i in reciver]))
    os.popen(sys_cmd)
