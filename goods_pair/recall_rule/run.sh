source /etc/bashrc


root_path=$1
#root_path="/user/chongweishen/recommend/pair_rec"

day=$2
#day=`date +"%Y-%m-%d" -d "-1 days"`

python Main.py -root_path $root_path -day $day 

rm comp_cata_sim
hadoop fs -getmerge $root_path/recall_rule/comp_cata_sim comp_cata_sim


