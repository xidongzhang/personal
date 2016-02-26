source /etc/bashrc

root_path="/user/chongweishen/recommend/pair_rec"
work_dir=`pwd`

cd $work_dir/get_train_data_for_personal;
sh myrun4test_new.sh $root_path subti test subti

cd $work_dir/rank_ml
rm test_subti.data
/hadoop/hadoop-1.2.1/bin/hadoop fs -getmerge $root_path/subti_test_data/round.four test_subti.data
./bin/predict test_subti.data subti_model_2015-12-18 subti_testing.result
cat subti_testing.result | auc/auc_map.py | sort -k1 -gr | auc/auc_reduce.py >> comp_test_auc
