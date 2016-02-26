#!/usr/local/bin/python
import redis
import util
import datetime
import os
from JobProducer import HadoopJobProducer

class DataHandler:
    redis_host = '10.8.245.1'
    redis_port = 2222
    
    local_data_file_name = 'data_from_hadoop'

    task_sign = 'twitter_lowest_price'

    redis_version_key = 'twitter_lowest_price_version'
    version = None
    redis_cli = redis.Redis(host = redis_host, port = redis_port, db = 0, socket_timeout = 1000)

    today = ''
    date_section = []

    old_data = {}

    count = 0
    
    admin = ['bowenzhang', 'chongweishen']

    def getOldDataVersion(self):
        version = self.redis_cli.get(self.redis_version_key)
        return version

    def start(self):
        self.version = self.getOldDataVersion()
        if self.version == None:
            self.coldBoot()
        else:
            self.warmBoot()
        print 'done'

    def setHiveDataToHDFS(self):
        for date_time_str in self.date_section:
            sys_cmd = "hadoop fs -ls /user/ml/rec_lowest_price_goods/data_from_hive/%s" % date_time_str
            res = os.popen(sys_cmd)
            if len(res.readlines()) > 0:
                continue
            sys_cmd = '''hive -e "insert overwrite directory '/user/ml/rec_lowest_price_goods/data_from_hive/%s' select specific_data['price'], query_data['twitter_id'] from mobile_app_log_new_orc where class_name='share' and method_name='main' and dt='%s'"  ''' % (date_time_str, date_time_str)
            os.popen(sys_cmd)

        return 0 
        
    def getDateSection(self, day_delta):
        date_time = datetime.datetime.now()
        self.today = date_time.strftime('%Y-%m-%d')
        yesterday = date_time + datetime.timedelta(days = -2)

        self.date_section = []

        if day_delta != 0:
            begin_day = yesterday - datetime.timedelta(days = day_delta)
            self.date_section = util.getDateList(begin_day.strftime('%Y%m%d'), yesterday.strftime('%Y%m%d'))
        else:
            self.date_section = [yesterday.strftime('%Y-%m-%d')]

    def sendMapReduceJob(self):
        job = HadoopJobProducer()
        job.setJobName("make_lowest_price_goods")
        job.setMapstr("python mapper.py")
        job.setReducestr("python reducer.py")
        job.setJobPriority("NORMAL")
        job.addFile(['mapper.py', 'reducer.py'])
        for date_time_str in self.date_section:
            job.addInput("/user/ml/rec_lowest_price_goods/data_from_hive/%s" % (date_time_str))
        job.setReduceNum(7)
        output_dir = "/user/ml/rec_lowest_price_goods/map_reduce_output/%s" % (self.today) 
        job.setOutput(output_dir)
        job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
        job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
        job.addOtherInfomation("-jobconf mapred.output.compress=false")
        os.popen("hadoop fs -rmr '%s'" % output_dir)
        return job.runJob()

    def getDataFromHDFS(self):
        os.popen('rm -f %s' % (self.local_data_file_name))
        os.popen('hadoop fs -getmerge /user/ml/rec_lowest_price_goods/map_reduce_output/%s %s' % (self.today, self.local_data_file_name))

    def loadOldData(self):
        self.old_data = self.redis_cli.hgetall('twitter_lowest_price_' + self.version)

    def writeDataToRedis(self):
        file_handle = open(self.local_data_file_name, 'r')
        while True:
            lines = file_handle.readlines(100000)
            if not lines:
                break
            for line in lines:
                t = line.split('\t')
                if len(t) < 2:
                    continue;
                twitter = t[0]
                price = int(t[1])
                if t[0] not in self.old_data:
                    self.old_data[twitter] = price
                elif t[1] < self.old_data[twitter]:
                    self.old_data[twitter] = price
        
        redis_buffer = {}
        self.res_count = 0
        redis_data_key = 'twitter_lowest_price_' + str(int(self.version) + 1)
        for twitter, price in self.old_data.items():
            redis_buffer[twitter] = str(price)
            self.res_count += 1
            if self.res_count % 1000 == 0:
                print "write to redis ... "  + str(self.res_count)
                succ = False
                while succ == False:
                    try:
                        self.redis_cli.hmset(redis_data_key, redis_buffer)
                        succ = True
                    except Exception, e:
                        print e
                        print 'retry redis'
                        succ = False
                redis_buffer = {}

        if len(redis_buffer) != 0:
            self.redis_cli.hmset(redis_data_key, redis_buffer)
            redis_buffer = {}
                

    def updateVersion(self):
        self.redis_cli.set(self.redis_version_key, str(int(self.version) + 1))
        try:
            self.redis_cli.delete('twitter_lowest_price_' + self.version)
        except Exception, e:
            pass

        if self.res_count < 1000:
            title = 'update %s failed' % (self.task_sign)
        else:
            title = 'update %s succ' % (self.task_sign)
        content = 'push %s data %s lines. version: %s' % (self.task_sign, self.res_count, str(int(self.version) + 1)) 
        util.sendMail(title, content, self.admin)
        

    def coldBoot(self):
        print 'coldBoot'
        self.version = '0'
        self.getDateSection(14)
        self.setHiveDataToHDFS()
        self.sendMapReduceJob()
        self.getDataFromHDFS()
        self.writeDataToRedis()
        self.updateVersion()
        
        
    def warmBoot(self):
        print 'warmBoot'
        self.getDateSection(14)
        self.setHiveDataToHDFS()
        self.sendMapReduceJob()
        self.getDataFromHDFS()
        #self.loadOldData()
        self.writeDataToRedis()
        self.updateVersion()

if __name__ == '__main__':
    handler = DataHandler()
    handler.start()
