#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"
argvDict={}

def proccessDate():
    tempOutput="/user/ml/personal/word2vec/action_seq"
    cmd = "hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("action_seq")
    job.setMapstr("python SeqMapper.py")
    job.setReducestr("python SeqReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["SeqMapper.py","SeqReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    
    dateStr=argvDict["date"]
    arr=dateStr.split(":")
    dateList=getDateList(arr[0],arr[1])
    for day in dateList:
        job.addInput("/user/ml/personal/user_profile/user_tag/user_action/$day/*", day, partName)
    job.setReduceNum(173)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput



if __name__ == '__main__':
    argvDict=initOption(sys.argv)
    printOption(argvDict)
   
    proccessDate()
    
    


