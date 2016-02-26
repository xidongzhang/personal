
basePath=$1
today=$2
yestoday=$3
sign=$4
hdfsData=$basePath/test_data/round.four

rm testing_data
#rm test_out_*
echo "hdfsData="$hdfsData
/hadoop/hadoop/bin/hadoop fs -getmerge $hdfsData testing_data
#./bin/predict testing_data l1_c4_model_$today test_out_$today
#cat test_out_$today | auc/auc_map.py | sort -k1 -gr | auc/auc_reduce.py >> test_auc_$today


