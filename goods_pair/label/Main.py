#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"

argvDict={}

def getCompFromOrder():
    rootPath=argvDict["root_path"]
    tempOutput="%s/label_data/comp_from_order"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("comp_from_order")
    job.setMapstr("python CompOrderMapper.py")
    job.setReducestr("python CompOrderReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["CompOrderMapper.py","CompOrderReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("%s/base_data/order_filt"%(rootPath),"",partName)
    job.setReduceNum(497)
    job.setOutput(tempOutput)
    #job.addOtherInfomation("-jobconf num.key.fields.for.partition=1")
    #job.addOtherInfomation("-jobconf stream.num.map.output.key.fields=2")
    #job.addOtherInfomation("-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def getCompFromAction():
    rootPath=argvDict["root_path"]
    tempOutput="%s/label_data/comp_from_action"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("comp_from_action")
    job.setMapstr("python CompActMapper.py")
    job.setReducestr("python CompActReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["CompActMapper.py","CompActReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("%s/base_data/user_act_session/*/*"%(rootPath),"",partName)
    job.setReduceNum(497)
    job.setOutput(tempOutput)
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


def getSubtiFromCart():
    rootPath=argvDict["root_path"]
    tempOutput="%s/label_data/subti_from_cart"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("subti_from_cart")
    job.setMapstr("python SubtiCartMapper.py")
    #job.setReducestr("python SubtiCartReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["SubtiCartMapper.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("%s/base_data/user_cart_session/*"%(rootPath),"",partName)
    job.setReduceNum(0)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def getSubtiFromLike():
    rootPath=argvDict["root_path"]
    tempOutput="%s/label_data/subti_from_like"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("subti_from_like")
    job.setMapstr("python SubtiLikeMapper.py")
    #job.setReducestr("python SubtiCartReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["SubtiLikeMapper.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("%s/base_data/user_like_session/*"%(rootPath),"",partName)
    job.setReduceNum(0)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def getSubtiFromAction():
    rootPath=argvDict["root_path"]
    tempOutput="%s/label_data/subti_from_action"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("subti_from_action")
    job.setMapstr("python SubtiActMapper.py")
    #job.setReducestr("python SubtiCartReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["SubtiActMapper.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    job.addInput("%s/base_data/user_act_session/*/*"%(rootPath),"",partName)
    job.setReduceNum(0)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput


if __name__ == '__main__':
    argvDict=initOption(sys.argv)
    printOption(argvDict)

    getCompFromOrder()
    getCompFromAction()
    getSubtiFromLike()
    getSubtiFromCart()
    getSubtiFromAction()


    sys.exit(0)


