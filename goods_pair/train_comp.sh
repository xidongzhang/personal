source /etc/bashrc


root_path=$1
day=$2
work_dir=`pwd`
version=$3

cd $work_dir/get_train_data_for_personal;
sh myrun_new.sh $root_path comp train 

cd $work_dir/rank_ml
sh train_comp.sh $root_path/comp_train_data/round.five $day >> train_comp_log 
cp comp_model_$day ../model_explain

cd $work_dir/model_explain
sh run.sh $root_path $day comp $version

