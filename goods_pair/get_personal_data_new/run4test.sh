
endDay=`date +"%Y%m%d" -d "-1 days"`
startDay=`date +"%Y%m%d" -d "-3 days"`
day=`date +"%Y-%m-%d" -d "-1 days"`

#/usr/local/bin/python sql.py "coral" "select cata_id,property_name from t_coral_property_name" "cata_id,property_name" > key_fea
basePath="/user/chongweishen/personal/rank_new"
colLimit=1
sign=test
hadoop fs -rmr $basePath/ctr_study_$sign/action_merge
hadoop fs -rmr $basePath/ctr_study_$sign/user_goods_info
hadoop fs -rmr $basePath/ctr_study_$sign/data_filt
python Main.py -date $startDay:$endDay -sign $sign -dayLimit 3 -colLimit $colLimit -attr_date $day -sample 100 -dayLast 2 -basePath "$basePath" -personal 1 -disConf "discretization.config.online"

cd ../get_train_data_for_personal;

sh myrun4test.sh $basePath $sign



