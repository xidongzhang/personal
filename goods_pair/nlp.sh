source /etc/bashrc

day=`date +"%Y-%m-%d" -d "0 days"`
root_path="/user/chongweishen/recommend/pair_rec"
work_dir=`pwd`

cd $work_dir/nlp;
sh run.sh $root_path $day




