#coding=gbk
import sys
import os
from JobProducer import *
from ArgvParser import *
import time
import datetime
from time import strftime, localtime
from datetime import timedelta, date

MAX_TRIES = 1
BASIC_PATH="/hadoop/hadoop"

argvDict={}

def getComTestPair():
    rootPath=argvDict["root_path"]
    one_day_ago = str(date.today()-timedelta(days=1))
    two_day_ago = str(date.today()-timedelta(days=2))
    three_day_ago = str(date.today()-timedelta(days=3))
    hql = '''
    hive -e "
        drop table tmp.recommend_pair_com;
        set hive.exec.compress.output=false;
        create table tmp.recommend_pair_com as select tid_scenario['twitter_id'] as tid1, list_scenario['source_tid'] as tid2, 'comp' as sample_type, '-' as tag1 , '-'     as tag2, 1 as frequence from tmp.dm_mobctr_useractionevent_txt where dt >= '%s' and dt <= '%s' and list_scenario['query']='index-match' and action_id = 'statistics-single_twitter' union all select tid_scenario['twitter_id'] as tid1, list_scenario['source_tid'] as tid2, 'com_no' as sample_type, '-' as tag1 , '-' as tag2, 1 as frequence from tmp.dm_mobctr_useractionevent_txt where dt >= '%s' and dt <= '%s'and list_scenario['query']='index-match' and action_id = 'search-tag_poster';
    "
    '''
    hql=hql%(three_day_ago, one_day_ago, three_day_ago, one_day_ago)
    #hql=hql%(one_day_ago, one_day_ago)
    #hql=hql%(one_day_ago, two_day_ago, three_day_ago, one_day_ago, two_day_ago, three_day_ago)
    os.system(hql)
    #tempOutput="%s/fea_data/tesing_pair_subti"%(rootPath)
    #cmd="hadoop fs -rmr %s/*"%(tempOutput)
    #os.system(cmd)
    #out_dir = "/user/hive/warehouse/tmp.db/recommend_pair_subti/*"
    #cmd="hadoop fs -cp %s %s"%(out_dir, tempOutput)
    #os.system(cmd)

def mergeFeaJob1():
    rootPath=argvDict["root_path"]
    tempOutput="%s/fea_data/test_com_pair_merge_fea1"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("pair_merge_fea1")
    job.setMapstr("python MergeFea1ComMapper.py")
    job.setReducestr("python MergeFea1Reducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["MergeFea1ComMapper.py","MergeFea1Reducer.py","goods_meta","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("/user/hive/warehouse/tmp.db/recommend_pair_com/*","",partName)
    job.addInput("%s/nlp/goods_fea_tag"%(rootPath),"",partName)
    #job.setReduceNum(0)
    job.setReduceNum(800)
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

def mergeFeaJob2():
    rootPath=argvDict["root_path"]
    tempOutput="%s/fea_data/test_com_pair_merge_fea2"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("pair_merge_fea2")
    job.setMapstr("python MergeFea2ComMapper.py")
    job.setReducestr("python MergeFea2Reducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["MergeFea2ComMapper.py","MergeFea2Reducer.py","goods_meta","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("%s/fea_data/test_com_pair_merge_fea1"%(rootPath),"",partName)
    job.addInput("%s/nlp/goods_fea_tag"%(rootPath),"",partName)
    job.setReduceNum(800)
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

def formatFea():
    rootPath=argvDict["root_path"]
    tempOutput="%s/fea_data/test_com_format_fea"%(rootPath)
    job = HadoopJobProducer()
    job.setJobName("format_fea")
    job.setMapstr("python FormatFeaMapper.py")
    job.setReducestr("python FormatFeaReducer.py")
    job.setJobPriority("HIGH")
    job.addFile(["FormatFeaMapper.py","FormatFeaReducer.py","goods_meta","cata_dict","can_comp_cata_dict","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    #job.addInput("%s/fea_data/no_relation_comp"%(rootPath), day, partName)
    #job.addInput("%s/fea_data/no_relation_subti"%(rootPath), day, partName)
    job.addInput("%s/fea_data/test_com_pair_merge_fea2"%(rootPath), day, partName)
    job.setReduceNum(800)
    job.setOutput(tempOutput)
    job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
    job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
    job.addOtherInfomation("-jobconf mapred.output.compress=false")
    #job.addOtherInfomation("-jobconf stream.memory.limit=1000")
    ret_1=job.runJob()
    if ret_1 != 0:
        sys.exit(-1)
    return tempOutput

def combineFea():
    rootPath=argvDict["root_path"]
    sign = argvDict["sign"]
    tempOutput="%s/fea_data/test_com_comb_fea"%(rootPath)
    sample = argvDict["sample"]
    colLimit = argvDict["colLimit"]
    job = HadoopJobProducer()
    job.setJobName("combine_fea")
    job.setMapstr("python CombFeaMapper.py %s %s"%(sign, sample))
    job.setReducestr("python CombFeaReducer.py %s"%(colLimit))
    #job.setCombinerstr("python CombFeaReducer.py 0")
    job.setJobPriority("HIGH")
    job.addFile(["CombFeaMapper.py","CombFeaReducer.py","goods_meta","goods_meta_used","utils.py","/hadoop/hive/lib/hive-exec-0.14.0.jar"])
    partName="*"
    if(argvDict.has_key("part")):
        partName=argvDict["part"]
    cmd="hadoop fs -rmr %s"%(tempOutput)
    os.system(cmd)

    day = argvDict["day"]
    job.addInput("%s/fea_data/test_com_format_fea"%(rootPath), day, partName)
    job.setReduceNum(800)
    #job.setReduceNum(0)
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

    getComTestPair()
    mergeFeaJob1()
    mergeFeaJob2()
    formatFea()
    combineFea()


    sys.exit(0)


