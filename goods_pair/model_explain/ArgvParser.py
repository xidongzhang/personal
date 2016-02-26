import datetime
import time

def initOption(argv):
    print argv
    now = datetime.datetime.now()
    yesterday = now + datetime.timedelta(days = -1)
    day=yesterday.strftime('%Y%m%d')


    argvDict={}
    for i in range(1,len(argv)-1,2):
        value = argv[i+1].replace("_day_",day)
        argvDict[argv[i][1:]]=value
    return argvDict

def printOption(argvDict):
    for key in argvDict:
        print (key + '='+ argvDict[key]),
        print "\t",
    print "\n\n"

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

def getMonthList(startDayStr,endDayStr):
    monList=[]
    endDay=int(endDayStr)
    startTm = time.strptime(startDayStr,"%Y%m%d")
    dateTime=datetime.datetime(startTm.tm_year,startTm.tm_mon,startTm.tm_mday)

    delta=0
    while True:
        day = dateTime + datetime.timedelta(days = delta)
        dayInt=int(day.strftime('%Y%m%d'))
        if(dayInt>endDay):
            break
        monthFormat=day.strftime('%Y-%m')
        if(monthFormat not in monList):
            monList.append(monthFormat)
        delta+=1
    return monList


#arr=getDateList("20141117","20150117")
#print arr



