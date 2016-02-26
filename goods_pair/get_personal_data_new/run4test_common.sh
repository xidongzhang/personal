
endDay=`date +"%Y%m%d" -d "-1 days"`
startDay=`date +"%Y%m%d" -d "-1 days"`
day=`date +"%Y-%m-%d" -d "-0 days"`

basePath=$1
colLimit=1
sign=$2
cur_path=$4
today=$5
yestoday=$6
attr_day=$7

hadoop fs -rmr $basePath/ctr_common_$sign/action_merge
hadoop fs -rmr $basePath/ctr_common_$sign/user_goods_info
hadoop fs -rmr $basePath/ctr_common_$sign/data_filt
python Main.py -date $startDay:$endDay -sign $sign -colLimit $colLimit -attr_date $attr_day -sample 100 -basePath "$basePath" -personal 1 -disConf "fea_conf2"

cd ../get_train_data_for_personal;

sh myrun4test.sh $basePath $sign $today

cd $cur_path/../rank_ml; sh get_test_data.sh $basePath $today $yestoday $sign




