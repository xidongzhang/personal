source /etc/bashrc

#hive -e "use tmp; insert overwrite directory '/user/chongweishen/personal/rank/ctr_study_train/user_action' select * from dm_mobctr_useractionevent where dt='2015-05-11'" ;
#hive -e "use tmp; drop table if exists scw_base_user_action; create table scw_base_user_action as select * from dm_mobctr_useractionevent where dt='2015-05-11'"
endDay=`date +"%Y%m%d" -d "-1 days"`
#startDay=`date +"%Y%m%d" -d "-53 days"`
startDay=`date +"%Y%m%d" -d "-31 days"`
day=`date +"%Y-%m-%d" -d "-1 days"`

basePath="/user/chongweishen/personal/rank_new"
sign=train
hadoop fs -rmr $basePath/ctr_study_$sign/action_merge
hadoop fs -rmr $basePath/ctr_study_$sign/user_goods_info
hadoop fs -rmr $basePath/ctr_study_$sign/data_filt

#colLimit=4
#rowLimit=2
#python Main.py -date $startDay:$endDay -dayLimit 53 -sign train -colLimit $colLimit -attr_date $day
colLimit=2
rowLimit=1
python Main.py -date $startDay:$endDay -dayLimit 31 -sign $sign -colLimit $colLimit -attr_date $day -sample 5 -dayLast 2 -basePath "$basePath" -personal 1 -disConf "discretization.config.online"

cd ../get_train_data_for_personal;

sh myrun_new.sh $colLimit $rowLimit $basePath $sign
cd ../model_explain
sh run.sh $basePath


