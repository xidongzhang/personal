source /etc/bashrc

day=`date +"%Y-%m-%d" -d "-1 days"`
root_path="/user/chongweishen/recommend/pair_rec"
#root_path="/user/ml/recommend/pair_rec"
work_dir=`pwd`

#cd $work_dir/nlp;
#sh run.sh $root_path $day

#cd $work_dir/index;
#sh run.sh $root_path $day


#cd $work_dir/fea_data;
#sh run.sh $root_path $day comp 20 4 1
#cd $work_dir/recall_rule;
#sh run.sh $root_path $day &


#cd $work_dir/fea_data;
#sh run.sh $root_path $day comp 90 4 0 
#sh run.sh $root_path $day subti 90 4 0
cd $work_dir/get_train_data_for_personal;

cd $work_dir;
sh train_comp.sh $root_path $day 
#cd $work_dir;
#sh train_subti.sh $root_path $day 




