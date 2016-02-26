#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"

argvDict={}

def getCompCataRule():
    rootPath=argvDict["root_path"]
    tempOutput="%s/recall_rule/comp_cata"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("comp_cata_co")
    job.setMapstr("python CompCataRuleMapper.py")
    job.setReducestr("python CompCataRuleReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["CompCataRuleMapper.py","CompCataRuleReducer.py","goods_meta","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("%s/fea_data/format_fea"%(rootPath), day, partName)
    job.setReduceNum(97)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)

    cmd = "rm comp_cata"
    os.system(cmd)
    cmd = "hadoop fs -getmerge %s comp_cata"%(tempOutput)
    os.system(cmd)
    return tempOutput

def calCompCata():
    rootPath=argvDict["root_path"]
    tempOutput="%s/recall_rule/comp_cata_sim"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("comp_cata_sim")
    job.setMapstr("python CalCompCataMapper.py")
    job.setReducestr("python CalCompCataReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["CalCompCataMapper.py","CalCompCataReducer.py","comp_cata","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("%s/recall_rule/comp_cata"%(rootPath), day, partName)
    job.setReduceNum(23)
    job.setOutput(tempOutput)
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

    getCompCataRule()
    calCompCata()

    sys.exit(0)


