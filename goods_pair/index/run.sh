source /etc/bashrc


root_path=$1
startDay=`date +"%Y-%m-%d" -d "-14 days"`
day=$2
day1=`date +"%Y-%m-%d" -d "-1 days"`
day2=`date +"%Y-%m-%d" -d "-2 days"`

hive -e "select twitter_id from ods_brd_goods_info where goods_status=1" > brd_goods

hive -e "use dm;select twitter_id, sum(show_nums),sum(click_nums) from ml_base_data where dt>='$startDay' and twitter_id>0 group by twitter_id" > show_click_file

avg=`cat show_click_file | awk -F "\t" '{show+=$2;click+=$3}END{print 0.9*click/show}'`
echo $avg

cat show_click_file | awk -F "\t" '{if($2>=500)print $1"\t"$3/$2}' > click_rate_dict
python getVerify.py brd_goods > verify_tid

forward=1
forward_index=forward_index_$day1
last_forward_index=forward_index_$day2
if [ ! -f "$forward_index" ]; then 
    forward=1
fi
python Main.py -root_path $root_path -day $day -avg $avg -forward $forward
rm direct_index
hadoop fs -getmerge $root_path/index/direct_index direct_index
rm inverted_index
hadoop fs -getmerge $root_path/index/inverted_index inverted_index

if [ ! -f "$forward_index" ]; then
    rm $forward_index
    hadoop fs -getmerge $root_path/index/forward_index $forward_index
fi
rm $last_forward_index
python merge_forward.py $forward_index

cp direct_index $forward_index inverted_index ../dict_4_pair/
cd ../dict_4_pair
mv $forward_index forward_index
sh run.sh



