#!/bin/bash
source ~/.bashrc

root_path="/user/chongweishen/recommend/pair_rec"
#root_path=$1

#i=1;
for((i=2;i<=30;i++));
do
    day=`date +"%Y-%m-%d" -d "-$i days"`
    hive -e "select order_id from ods_focus_fake_orders where is_fake=1 and dt='$day'" > fake/fake_orders_$day
    cp fake/fake_orders_$day fake_orders

    hive -e "use dm; ALTER TABLE ml_user_action_4_pair DROP PARTITION (dt='$day');"

    hive -e "add file fake_orders; add file get_user_action.py; 
        insert overwrite table dm.ml_user_action_4_pair partition (dt='$day')
        select transform(date, hour, minute, second, user_id, action_id, list_scenario['query'], list_scenario['offset']
        , list_scenario['pos'], tid_scenario['twitter_id'], tid_scenario['order_id']) using 'get_user_action.py' as (time, user_id
        , action, twitter_id, query, twitter_pos) from tmp.dm_mobctr_useractionevent_txt where dt='$day'"
    python MainBatch.py -root_path $root_path -day $day
done;





