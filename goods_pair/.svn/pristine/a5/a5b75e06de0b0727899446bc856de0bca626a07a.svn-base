#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"

def addiDict():
    tempOutput="%s/nlp/addi_word"%(argvDict["root_path"])
    cmd = "hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("addi_word")
    job.setMapstr("python AddiMapper.py")
    job.setReducestr("python AddiReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["AddiMapper.py","AddiReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    job.addInput("/user/hive/warehouse/ods_brd_goods_info/*","",partName)
    job.setReduceNum(297)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput



def tfidfDict():
    tempOutput="%s/nlp/tfidf"%(argvDict["root_path"])
    cmd = "hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("goods_idf")
    job.setMapstr("python TfidfMapper.py")
    job.setReducestr("python TfidfReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["TfidfMapper.py","TfidfReducer.py","goods_meta","addi_dict","segUtils.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    day = argvDict["day"]
    job.addInput("/user/hive/warehouse/dm.db/ml_goods_info/dt=$day", day, partName)
    job.setReduceNum(497)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def goodsTag():
    tempOutput="%s/nlp/goods_fea_tag"%(argvDict["root_path"])
    cmd = "hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("goods_idf")
    job.setMapstr("python TagMapper.py")
    #job.setReducestr("python TagReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["TagMapper.py","TagReducer.py","goods_meta","addi_word","cata_dict","segUtils.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    day = argvDict["day"]
    job.addInput("/user/hive/warehouse/dm.db/ml_goods_info/dt=$day", day, partName)
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

def getAddiWord():
    cmd = "rm addi_result"
    os.system(cmd)
    cmd = "hadoop fs -getmerge %s/nlp/addi_word addi_result"%(argvDict["root_path"])
    os.system(cmd)
    cmd = "cat addi_result | awk -F \"\t\" '{if($3>=20 && length($1)>1)print $0}' | sort -k3nr -t \"	\" > addi_word"
    os.system(cmd)

argvDict={}
if __name__ == '__main__':
    argvDict=initOption(sys.argv)
    printOption(argvDict)

    addiDict()
    ###tfidfDict()
    getAddiWord()
    goodsTag()
    sys.exit(0)


