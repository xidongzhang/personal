#!/usr/local/bin/python
import redis
import util
import datetime
import os
from JobProducer import HadoopJobProducer

class DataHandler:
    redis_host = '10.8.245.1'
    redis_port = 2222

    task_sign = os.path.basename(__file__).split('.')[0]
    redis_version_key = 'user_focus_twitters_version'
    version = None
    redis_cli = redis.StrictRedis(host = redis_host, port = redis_port, db = 0, socket_timeout = 100)

    today = ''
    date_section = []

    old_data = {}

    res_count = 0

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
            sys_cmd = "hadoop fs -ls /user/ml/user_focus_twitters/data_from_hive/%s" % date_time_str
            res = os.popen(sys_cmd)
            if len(res.readlines()) > 0:
                continue
            sys_cmd = '''hive -e "insert overwrite directory '/user/ml/user_focus_twitters/data_from_hive/%s' select user_id, query_data['twitter_id'] from mobile_app_log_new_orc where class_name='share' and method_name='details' and dt='%s'"  ''' % (date_time_str, date_time_str)
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
        job.setJobName("user_focus_twitters")
        job.setMapstr("python mapper.py")
        job.setReducestr("python reducer.py")
        job.setJobPriority("NORMAL")
        job.addFile(['mapper.py', 'reducer.py'])
        for date_time_str in self.date_section:
            job.addInput("/user/ml/user_focus_twitters/data_from_hive/%s" % (date_time_str))
        job.setReduceNum(7)
        output_dir = "/user/ml/user_focus_twitters/map_reduce_output/%s" % (self.today) 
        job.setOutput(output_dir)
        job.addOtherInfomation("-jobconf mapred.job.map.capacity=800")
        job.addOtherInfomation("-jobconf mapred.job.reduce.capacity=800")
        job.addOtherInfomation("-jobconf mapred.output.compress=false")
        os.popen("hadoop fs -rmr '%s'" % output_dir)
        return job.runJob()

    def getDataFromHDFS(self):
        os.popen('rm -f data_from_hadoop')
        os.popen('hadoop fs -getmerge /user/ml/user_focus_twitters/map_reduce_output/%s data_from_hadoop' % (self.today))

    def loadOldData(self):
        self.old_data = self.redis_cli.hgetall('user_focus_twitters_' + self.version)
        for key, val in self.old_data.items():
            self.old_data[key] = set(val.split(','))

    def writeDataToRedis(self):
        file_handle = open("data_from_hadoop", 'r')
        while True:
            lines = file_handle.readlines(100000)
            if not lines:
                break
            for line in lines:
                t = line.strip().split('\t')
                if len(t) < 2 or t[1] == '':
                    continue;
                user = t[0]
                tids = set(t[1].split(','))
                if user not in self.old_data:
                    self.old_data[user] = tids
                else: 
                    self.old_data[user] |= tids
        
        redis_buffer = {}
        self.res_count = 0
        redis_data_key = 'user_focus_twitters_' + str(int(self.version) + 1)
        for user, twitter_set in self.old_data.items():
            redis_buffer[user] = ','.join([i for i in twitter_set])
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
                        print "retry redis"
                        succ = False
                    
                redis_buffer = {}

        if len(redis_buffer) != 0:
            self.redis_cli.hmset(redis_data_key, redis_buffer)
            redis_buffer = {}
                

    def updateVersion(self):
        self.redis_cli.set(self.redis_version_key, str(int(self.version) + 1))
        try:
            self.redis_cli.delete('user_focus_twitters_' + self.version)
        except:
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
