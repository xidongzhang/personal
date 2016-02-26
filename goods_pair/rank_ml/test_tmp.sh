
testing_data=$1
model=$2 
test_out=$3
test_auc=$4

./bin/predict $testing_data $model $test_out
cat $test_out | auc/auc_map.py | sort -k1 -gr | auc/auc_reduce.py >> $test_auc


