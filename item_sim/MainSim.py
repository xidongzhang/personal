#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"
argvDict={}

def userActionMerge():
    tempOutput="/user/chongweishen/personal_4_pair/item_sim/user_action_merge"
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("user_action_merge_for_sim")
    job.setMapstr("python MergeMapper.py")
    job.setReducestr("python MergeReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["MergeMapper.py","MergeReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    
    job.addInput("/user/chongweishen/user_tag/common/order_filt/*","",partName)
    job.addInput("/user/chongweishen/user_tag/common/user_like/*","",partName)
    job.addInput("/user/hive/warehouse/ods_bat_shopcart/dt=201*/*","",partName)
    
    dateStr=argvDict["date"]
    arr=dateStr.split(":")
    dateList=getDateList(arr[0],arr[1])
    for day in dateList:
        job.addInput("/user/chongweishen/personal/logs/*/$day/*",day,partName)
    #job.addInput("/user/chongweishen/personal/logs/*/2015-01-11/*","",partName)
    #job.addInput("/user/hive/warehouse/ods_bat_shopcart/dt=2015-01-11/*","",partName)
    job.setReduceNum(747)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def goodsPv():
    tempOutput="/user/chongweishen/personal_4_pair/item_sim/goods_pv"
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("goods_pv")
    job.setMapstr("python PvMapper.py")
    job.setReducestr("python PvReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["PvMapper.py","PvReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]

    job.addInput("/user/chongweishen/personal_4_pair/item_sim/user_action_merge/*","",partName)
    job.setReduceNum(747)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def actionSort():
    tempOutput="/user/chongweishen/personal_4_pair/item_sim/action_sort"
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("action_sort")
    job.setMapstr("python SortMapper.py")
    #job.setReducestr("python SortReducer.py")
    job.setReducestr("cat")
    job.setJobPriority("NORMAL")
    job.addFile(["SortMapper.py","pv_dict","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]

    job.addInput("/user/chongweishen/personal_4_pair/item_sim/user_action_merge/*","",partName)
    job.setReduceNum(1347)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput


def combineGoods():
    tempOutput="/user/chongweishen/personal_4_pair/item_sim/combine"
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("combine_goods")
    job.setMapstr("python CombineMapper.py")
    job.setReducestr("python CombineReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["CombineMapper.py","CombineReducer.py","utils.py","pv_dict","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]

    job.addInput("/user/chongweishen/personal_4_pair/item_sim/user_action_merge/*","",partName)
    job.setReduceNum(797)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=2000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def getCo():
    tempOutput="/user/chongweishen/personal_4_pair/item_sim/goods_co"
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("goods_co")
    job.setMapstr("python CoMapper.py")
    job.setReducestr("python PvReducer.py")
    job.setCombinerstr("python CoCombiner.py")
    job.setJobPriority("NORMAL")
    job.addFile(["CoMapper.py","CoCombiner.py","PvReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]

    job.addInput("/user/chongweishen/personal_4_pair/item_sim/action_sort/*","",partName)
    job.setReduceNum(1597)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.map.tasks=1000")
    job.addOtherInfomation("-jobconf mapred.min.split.size=33554432")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=1200")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=1200")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def formatCo():
    tempOutput="/user/chongweishen/personal_4_pair/item_sim/format_co_final"
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("format_co")
    job.setMapstr("python FormatMapper.py")
    job.setReducestr("python FormatReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["FormatMapper.py","FormatReducer.py","utils.py","pv_dict","goods_cata","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]

    job.addInput("/user/chongweishen/personal_4_pair/item_sim/goods_co/*","",partName)
    job.setReduceNum(797)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=2000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def getCata():
    tempOutput="/user/chongweishen/personal_4_pair/item_sim/goods_cata"
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("goods_cata")
    job.setMapstr("python CataMapper.py")
    #job.setReducestr("python FormatReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["CataMapper.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]

    job.addInput("/user/chongweishen/goods_merge/*","",partName)
    job.setReduceNum(0)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)

    cmd="rm goods_cata"
    os.system(cmd)
    cmd="hadoop fs -getmerge /user/chongweishen/personal_4_pair/item_sim/goods_cata goods_cata"
    os.system(cmd)

    return tempOutput


if __name__ == '__main__':
    argvDict=initOption(sys.argv)
    printOption(argvDict)

    userActionMerge()
    goodsPv() 
    cmd="sh filt.sh"
    os.system(cmd)
###    #combineGoods()
    actionSort()
    getCo()
    getCata()

    formatCo()
    sys.exit(0)


