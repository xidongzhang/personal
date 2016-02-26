#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"
argvDict={}

def getFeaIndex():
    fin=open(argvDict["model"])
    fout=open(argvDict["fea_index"],"w")
    blank=6
    i=0
    for line in fin:
        if(i<blank):
            i+=1
            continue
        line=line.strip()
        if(line=="" or float(line)==0.0):
            i+=1
            continue
        print >> fout, "%s\t%s"%(line,i-blank+1)
        #print >> fout, "%s\t%s"%(round(float(line),5),i-blank+1)
        i+=1
    fout.close()
    fin.close()


def modelExplain():
    tempOutput=argvDict["outputPath"]
    hour=None
    if(argvDict.has_key("hour")):
        hour=argvDict["hour"]
    job = HadoopJobProducer()
    job.setJobName("model_explain")
    job.setMapstr("python ExplainMapper.py")
    job.setReducestr("python ExplainReducer.py")
    job.setJobPriority("NORMAL")
    job.addFile(["ExplainMapper.py","ExplainReducer.py","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    inputPath=argvDict["inputPath"]
    #job.addInput("/user/ml/personal_new/training_data/fea2id.txt","",partName)
    job.addInput(inputPath,"",partName)
    sign = argvDict["sign"]
    job.addInput("/user/ml/goods_pair/%s/fea_index"%(sign),"",partName)
    job.setReduceNum(453)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=1000")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    job.addOtherInfomation("-jobconf mapred.min.split.size=67108864")
    job.addOtherInfomation("-jobconf mapred.map.tasks=200")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput


if __name__ == '__main__':
    argvDict=initOption(sys.argv)
    printOption(argvDict)
    sign = argvDict["sign"]

    getFeaIndex()
    cmd="hadoop fs -rmr /user/ml/goods_pair/%s/fea_index/*"%(sign)
    print cmd
    os.system(cmd)
    cmd="hadoop fs -put fea_index_%s /user/ml/goods_pair/%s/fea_index"%(sign,sign)
    print cmd
    os.system(cmd)
    modelExplain()

    sys.exit(0)


