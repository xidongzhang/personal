source /etc/bashrc

cur_path=`pwd`
endDay=`date +"%Y%m%d" -d "-0 days"`
#startDay=`date +"%Y%m%d" -d "-53 days"`
startDay=`date +"%Y%m%d" -d "-33 days"`
day=`date +"%Y-%m-%d" -d "-0 days"`
today=`date +"%Y%m%d" -d "-0 days"`
yestoday=`date +"%Y%m%d" -d "-1 days"`

basePath="/user/ml/personal/rank_pest"
sign=train
testSign=test
sample=100

hadoop fs -rmr $basePath/ctr_common_$sign/action_merge
hadoop fs -rmr $basePath/ctr_common_$sign/user_goods_info
hadoop fs -rmr $basePath/ctr_common_$sign/data_filt

colLimit=2
rowLimit=1
python Main.py -date $startDay:$endDay -sign $sign -colLimit $colLimit -attr_date $day -sample 5 -basePath "$basePath" -personal 1 -disConf "fea_conf2"

cd $cur_path/../get_train_data_for_personal;

sh myrun_new.sh $colLimit $rowLimit $basePath $sign $sample $today 


cd $cur_path
sh run4test_common.sh $basePath $testSign $sign $cur_path $today $yestoday $day &        

cd $cur_path/../rank_ml; sh train.sh $basePath $today $yestoday $sign >> trainlog 

cd $cur_path/../model_explain
sh run.sh $basePath $today $yestoday & 

cd $cur_path/../rank_ml; sh test.sh $basePath $today $yestoday $testSign >> testlog 

cd $cur_path
mv $cur_path/../model_explain/model_explain_$today $cur_path/../model/plain.model
cat  $cur_path/../rank_ml/test_auc_$today >> $cur_path/../model/auc




