source /etc/bashrc

root_path="/user/chongweishen/recommend/pair_rec"
work_dir=`pwd`

cd $work_dir/get_train_data_for_personal;
sh myrun4test_new.sh $root_path com test comp

cd $work_dir/rank_ml
rm -rf test_comp.data && /hadoop/hadoop-1.2.1/bin/hadoop fs -getmerge $root_path/com_test_data/round.four test_comp.data
./bin/predict test_comp.data comp_model_2015-12-21 comp_testing.result
cat comp_testing.result | auc/auc_map.py | sort -k1 -gr | auc/auc_reduce.py >> comp_test_auc
