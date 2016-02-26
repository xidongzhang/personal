day=`date +"%Y-%m-%d" -d "-1 days"`
root_path="/user/chongweishen/recommend/pair_rec"
work_dir=`pwd`
sign="comp"
sample=90
colLimit=4
python TestMainCom.py -root_path $root_path -day $day -sign $sign -sample $sample -colLimit $colLimit -runpair 0
