#!/bin/bash

#产生中间数据 input:起止时间两个参数
function generMidData(){	
	start_dt=$1
	end_dt=$2
	hive -hivevar start_dt=$start_dt --hivevar end_dt=$end_dt -f generMidData.version2.sql
}
#产生下游使用的样本数据
function generSamplesData(){	
	for ((i = 0;i < 10;i++ )); do
		echo "$i 次循环"
		nohup hive -hivevar hash_uid="$i" -f samples2hive.sql >hql_samples_${i}.log 2>&1 &
	done
}
#校验数据
function checkData(){	
	check_sql="
	select sample_type,hash_uid,sum(1) as num 
	from tmp.collocations_samples
	where sample_type in ('negative','positive')
	group by sample_type,hash_uid"
	echo -e "$check_sql"
	flag=0
	for (( i = 0; i < 3; i++ )); do
		#循环执行3次共 3*20m = 1小时内
		hive -e "$check_sql">check_file
		rownum=`wc -l check_file|cut -d " " -f1`
		if [ $rownum -eq 20 ]; then
			#满足正负样本各10个hash_uid共20个hash_uid
			flag=1
			break
		else
			sleep 20m
		fi
	done
	if [ "$flag" -eq 0 ]; then
		errorMsg="collocations_samples date Error NOT ENOUGH!!!"
		echo "$errorMsg"
		mail -s "$errorMsg" xidongzhang@meilishuo.com
		exit 0
	else
		msg="collocations samples SUCCUSS!!!"
		echo "$msg"
	fi
}
#==============start==here=================

bin=`dirname "$0"`
bin=`cd $bin;pwd`
cd $bin

start_dt="2015-10-01"

if [ $# -ge 1 ];then
	end_dt=$1
else
	end_dt=`date -d '-1 day' +'%Y-%m-%d'`
fi
intervalSeconds=`expr $(date -d "$end_dt" +'%s') - $(date -d "$start_dt" +'%s')`
while [ $intervalSeconds -le 0 ]
do
	read -p "截止日期需要在2015-10-01之后，请重新输入截止日期 :" end_dt
	intervalSeconds=`expr $(date -d "$end_dt" +'%s') - $(date -d "$start_dt" +'%s')`
done
echo "start_dt : $start_dt"
echo "end_dt : $end_dt"
#调用函数
generMidData $start_dt $end_dt
generSamplesData 
#产生样本数据的10个并发进程正在运行 sleep 10mins再进行数据检验
sleep 10m
checkData


