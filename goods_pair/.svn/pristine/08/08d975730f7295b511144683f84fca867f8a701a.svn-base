start_dt=`date +"%Y-%m-%d" -d "-75 days"`
end_dt=`date +"%Y-%m-%d"`

#echo $start_dt
#echo $end_dt
#产生中间数据
#hive -hivevar start_dt=$start_dt -hivevar end_dt=$end_dt -f GenerateMidData.sql
#产生下游使用的样本数据
hive -e "DROP table tmp.comp_samples;"
for ((i = 0;i < 10;i++ )); do
	echo "$i 次循环"
	hive -hivevar hash_uid="$i" -f Dump2Hive.sql
done
