#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"

argvDict={}

def invertedIndex():
    rootPath=argvDict["root_path"]
    tempOutput="%s/index/inverted_index"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("inverted_index")
    job.setMapstr("python InvertedMapper.py")
    job.setReducestr("python InvertedReducer.py %s"%(argvDict["avg"]))
    job.setJobPriority("HIGH")
    job.addFile(["InvertedMapper.py","InvertedReducer.py","click_rate_dict","verify_tid","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("%s/index/forward_index"%(rootPath),"",partName)
    job.setReduceNum(497)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def forwardIndex():
    rootPath=argvDict["root_path"]
    tempOutput="%s/index/forward_index"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("forward_index")
    job.setMapstr("python ForwardMapper.py")
    job.setJobPriority("HIGH")
    job.addFile(["ForwardMapper.py","goods_meta","utils.py","brd_goods","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("%s/nlp/goods_fea_tag"%(rootPath),"",partName)
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

def directIndex():
    rootPath=argvDict["root_path"]
    tempOutput="%s/index/direct_index"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("direct_index")
    job.setMapstr("python DirectMapper.py")
    job.setReducestr("python DirectReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["DirectMapper.py","DirectReducer.py","brd_goods","goods_meta","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("%s/fea_data/format_fea"%(rootPath),"",partName)
    job.setReduceNum(497)
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

    if(argvDict["forward"] == "1"):
        forwardIndex()
    invertedIndex()
    directIndex()

    sys.exit(0)


