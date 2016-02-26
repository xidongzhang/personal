source /etc/bashrc

#sh get_brd.sh
#cp ../user_tag_new/audio_dict brd_goods

hive -e "select twitter_id from ods_brd_goods_info where goods_status=1" > brd_goods

hadoop fs -rmr /user/chongweishen/personal_4_pair/item_sim/*

day=`date +"%Y%m%d" -d "-1 days"`
startDay=`date +"%Y%m%d" -d "-30 days"`
echo $startDay:$day

/usr/local/bin/python MainSim.py -date $startDay:$day
rm format_co_final;
hadoop fs -getmerge /user/chongweishen/personal_4_pair/item_sim/format_co_final format_co_final

sh push_sim.sh

