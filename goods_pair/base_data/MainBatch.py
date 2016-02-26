#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"

argvDict={}

def userActSession():
    day=argvDict["day"]
    rootPath=argvDict["root_path"]
    tempOutput="%s/base_data/user_act_session/%s"%(rootPath,day)
    job = HadoopJobProducer()
    job.setJobName("user_act_4_pair")
    job.setMapstr("python ActSessionMapper.py")
    job.setReducestr("python ActSessionReducer.py")
    job.setJobPriority("VERY_HIGH")
    job.addFile(["ActSessionMapper.py","ActSessionReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("/user/hive/warehouse/dm.db/ml_user_action_4_pair/dt=$day",day,partName)
    job.setReduceNum(87)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def orderFilt():
    rootPath=argvDict["root_path"]
    tempOutput="%s/base_data/order_filt"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("order_filt_4_pair")
    job.setMapstr("python OrderFiltMapper.py")
    job.setReducestr("python OrderFiltReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["OrderFiltMapper.py","OrderFiltReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)
    job.addInput("/user/hive/warehouse/ods_bat_order/dt=201*/*","",partName)
    job.addInput("/user/hive/warehouse/ods_focus_fake_orders/*/*","",partName)
    job.addInput("/user/hive/warehouse/ods_bat_goods_map/dt=201*/*","",partName)
    job.setReduceNum(97)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def getUserLike():
    rootPath=argvDict["root_path"]
    tempOutput="%s/base_data/user_like_session"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("user_like_4_pair")
    job.setMapstr("python UserLikeMapper.py")
    job.setReducestr("python UserLikeReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["UserLikeMapper.py","UserLikeReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("/user/hive/warehouse/ods_dolphin_twitter_info/dt=2014*/*","",partName)
    job.addInput("/user/hive/warehouse/ods_dolphin_twitter_info/dt=2015*/*","",partName)
    job.setReduceNum(497)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def getUserCart():
    rootPath=argvDict["root_path"]
    tempOutput="%s/base_data/user_cart_session"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("user_cart_4_pair")
    job.setMapstr("python UserCartMapper.py")
    job.setReducestr("python UserCartReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["UserCartMapper.py","UserCartReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("/user/hive/warehouse/ods_bat_shopcart/dt=2015*/*","",partName)
    job.addInput("/user/hive/warehouse/ods_bat_shopcart/dt=2014*/*","",partName)
    job.setReduceNum(597)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-cacheArchive \"/user/chongweishen/tool/data.tar.gz#Data\"")
    job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput


if __name__ == '__main__':
    argvDict=initOption(sys.argv)
    printOption(argvDict)

    userActSession();
#    orderFilt()
#    getUserLike() 
#    getUserCart()

    sys.exit(0)


