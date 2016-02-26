#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"
argvDict={}
#sign="train"
basePath=""

def userActionMerge():
    #tempOutput="/user/chongweishen/personal/rank/ctr_common_%s/action_merge"
    tempOutput="%s/ctr_common_%s/action_merge"%(basePath,sign)
    #tempOutput=tempOutput%(sign)
    #dayLimit=argvDict["dayLimit"]
    dayLimit=100  # no used
    #dayLast=argvDict["dayLast"]
    dayLast=0   #no used
    sample=argvDict["sample"]
    actLimit=600
    if("actLimit" in argvDict):
        actLimit=argvDict["actLimit"]
    personal=argvDict["personal"]
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("user_action_merge")
    #job.setMapstr("python MergeMapper.py")
    job.setMapstr("python MergeMapper.py %s %s %s %s %s"%(dayLimit,dayLast,sample,personal,sign))
    job.setReducestr("python MergeReducer.py {/c} 0")
    job.setJobPriority("NORMAL")
    job.addFile(["MergeMapper.py","MergeReducer.py","action_meta","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    dateStr=argvDict["date"]
    arr=dateStr.split(":")
    dateList=getDateList(arr[0],arr[1])
    for dateStr in dateList:
	#if(dateStr=="2015-07-02"):
	#    continue
    #monList=getMonthList(arr[0],arr[1])
    #for month in monList:
    #    job.addInput("/user/hive/warehouse/tmp.db/dm_mobctr_useractionevent/dt=$day-*",month,partName)
        job.addInput("/user/hive/warehouse/tmp.db/dm_mobctr_useractionevent_txt/dt=$day",dateStr,partName)
    #job.addInput("/user/hive/warehouse/tmp.db/dm_mobctr_useractionevent/dt=2015-05-12",dateStr,partName)
    #job.addInput("/user/hive/warehouse/tmp.db/scw_base_user_action/*",dateStr,partName)
    
    job.setReduceNum(1389)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    #job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    #job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    #job.addOtherInfomation("-inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat")
    job.addOtherInfomation("-jobconf mapreduce.job.split.metainfo.maxsize=100000000")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")

    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def userGoods():
    #tempOutput="/user/chongweishen/personal/rank/ctr_common_%s/user_goods_info"
    tempOutput="%s/ctr_common_%s/user_goods_info"%(basePath,sign)
    #tempOutput=tempOutput%(sign)
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("user_goods_info")
    job.setMapstr("python UserGoodsMapper.py")
    job.setReducestr("python UserGoodsReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["UserGoodsMapper.py","UserGoodsReducer.py","title_word_count_sort","goods_need_meta","goods_meta"
        ,"utils.py","segUtils.py","dict.txt","jieba.mod","conf.txt","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    day=argvDict["attr_date"]
    #job.addInput("/user/chongweishen/personal/rank/goods_attr_format/$day",day,partName)
    job.addInput("/user/hive/warehouse/tmp.db/ml_goods_info/dt=$day",day,partName)
    job.addInput("%s/ctr_common_%s/action_merge"%(basePath,sign),"",partName)
    job.setReduceNum(1949)
    job.setOutput(tempOutput)
#job.addOtherInfomation("-jobconf mapred.min.split.size=33554432")
    job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/wsscwqx_python.tar.gz#python\"")
    job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.map.tasks=6500")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def dataFilt():
    #tempOutput="/user/chongweishen/personal/ctr_common_%s/data_filt_%s"%(argvDict["tmp"])
    tempOutput="%s/ctr_common_%s/data_filt"%(basePath,sign)
    #tempOutput="/user/chongweishen/personal/rank/ctr_common_train/data_filt"
    hour=None
    colLimit=4
    if("colLimit" in argvDict):
        colLimit=argvDict["colLimit"]
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("dis_fea")
    job.setMapstr("python FiltMapper.py %s"%(argvDict["disConf"]))
    job.setReducestr("python FiltReducer.py %s %s"%(colLimit,argvDict["disConf"]))
    job.setJobPriority("NORMAL")
    job.addFile(["FiltMapper.py","FiltReducer.py","jieba.mod","dict.txt",argvDict["disConf"],"utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    
    job.addInput("%s/ctr_common_%s/user_goods_info"%(basePath,sign),"",partName)
    job.setReduceNum(4847)
    #job.setReduceNum(0)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.min.split.size=67108864")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.output.compress=true")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def dataFilt4Test():
    #tempOutput="/user/chongweishen/personal/ctr_common_%s/data_filt_%s"%(argvDict["tmp"])
    #tempOutput="/user/chongweishen/personal/rank/ctr_common_test/data_filt"
    tempOutput="%s/ctr_common_%s/data_filt"%(basePath,sign)
    colLimit=4
    if("colLimit" in argvDict):
        colLimit=argvDict["colLimit"]
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    personal=argvDict["personal"]
    job = HadoopJobProducer()
    job.setJobName("dis_fea_test")
    #job.setMapstr("python FiltMapper4Test.py")
    #job.setReducestr("python FiltReducer4Test.py")
    job.setMapstr("python FiltMapper.py %s %s"%(argvDict["disConf"],personal))
    job.setReducestr("python FiltReducer.py %s %s"%(colLimit,argvDict["disConf"]))
    job.setJobPriority("NORMAL")
    job.addFile(["FiltMapper.py","FiltReducer.py","jieba.mod","dict.txt",argvDict["disConf"],"utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    
    job.addInput("%s/ctr_common_%s/user_goods_info"%(basePath,sign),"",partName)
    job.setReduceNum(2847)
    #job.setReduceNum(0)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.min.split.size=67108864")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.output.compress=true")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput


if __name__ == '__main__':
    argvDict=initOption(sys.argv)
    printOption(argvDict)
    sign=argvDict["sign"]
    basePath=argvDict["basePath"]

    userActionMerge()
    #formatGoodsAttr()
    userGoods()
    
    if(sign=="train"):
        dataFilt()
    elif(sign=="test"):
        dataFilt4Test()
    




