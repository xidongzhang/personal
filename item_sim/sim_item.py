#! /usr/bin/python

import sys
import redis
import time
import urllib

'''
bj_redis_ip_master="172.16.15.117"
bj_redis_port_master="6385"
gz_redis_ip_master="10.0.14.33"
gz_redis_port_master="6385"
bj_redis_ip_slave="172.16.15.117"
bj_redis_port_slave="6385"
gz_redis_ip_slave="10.0.14.33"
gz_redis_port_slave="6385"
'''

bj_redis_ip_master="10.8.5.22"
bj_redis_port_master="6380"
gz_redis_ip_master="10.0.18.51"
gz_redis_port_master="6380"
bj_redis_ip_slave="10.8.5.22"
bj_redis_port_slave="6380"
gz_redis_ip_slave="10.0.18.51"
gz_redis_port_slave="6380"


def load_sim(sim_path):
    simDict = {}
    sim_file = open(sim_path)
    i = 0
    for line in sim_file:
        items = line.strip().split()
        if len(items) != 2:
            continue
        arr = items[1].split(",")
        iList = []
        for iStr in arr:
            iList.append(iStr[0:iStr.find(":")])
        simDict[items[0]] = ",".join(iList[0:15])
        i += 1
        if i%100000 == 0:
            print "load sim %d ..."%(i)
    sim_file.close()
    return simDict

if __name__ == "__main__":
    ip=bj_redis_ip_master
    port=bj_redis_port_master
    if(sys.argv[1]=="gz"):
        ip=gz_redis_ip_master
        port=gz_redis_port_master

    simDict = load_sim(sys.argv[2])
    
    print len(simDict)
    redisPrefix="greate_item_sim_"
    if True:
        redis_client = redis.StrictRedis(host=ip, port=port)
        if(redisPrefix+'version' not in redis_client):
            redis_client[redisPrefix+'version']=1
        cur_version = int(redis_client[redisPrefix+'version'])
        print redisPrefix+str(cur_version)
        i = 0
        while i < cur_version :
            redis_client.delete(redisPrefix+str(i))
            i += 1
        cur_version += 1
        print redisPrefix+str(cur_version)
        i = 0
        tmp = {}
        for key, value in simDict.items():
            tmp[key] = value
            i += 1
            if i % 10000 == 0:
                redis_client.hmset(redisPrefix+str(cur_version), tmp)
                tmp.clear()
                time.sleep(0.1)
            if i % 50000 == 0:
                print "finish push %d ..."%(i)
        redis_client.hmset(redisPrefix+str(cur_version), tmp)
        redis_client.set(redisPrefix+'version', cur_version)
        cur_version = int(redis_client[redisPrefix+'version']) 
        print redisPrefix+str(cur_version)
    
