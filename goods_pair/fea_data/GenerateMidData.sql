--======================
--有购买的session基础数据
--======================

DROP TABLE tmp.tmp_comp_samples_basedata;
CREATE TABLE if not exists `tmp.tmp_comp_samples_basedata`(
  `is_buy_query` bigint, 
  `user_id` string, 
  `twitter_id` string, 
  `real_show_nums` int, 
  `click_nums` int, 
  `real_pay_nums` int, 
  `md5query` string)
PARTITIONED BY ( 
  hash_uid tinyint);



--准备session基础数据 减少二次运算成本
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions.pernode = 2500;
SET hive.exec.max.dynamic.partitions=2000;
insert overwrite table tmp.tmp_comp_samples_basedata partition(hash_uid)
select * from
(select sum(real_pay_nums) over (partition by dt,hash_uid,user_id,md5query) as is_buy_query, user_id, twitter_id, real_show_nums, click_nums, real_pay_nums, md5query, hash_uid from (select *, md5(query) as md5query, substr(hex(md5(user_id)),64,1) as hash_uid from dm.ml_base_data where dt >'${start_dt}' and dt<'${end_dt}' and user_id>0 ) t ) tmp where is_buy_query>0 and real_show_nums>0;
