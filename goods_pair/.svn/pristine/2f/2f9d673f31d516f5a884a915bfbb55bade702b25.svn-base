--======================
--有购买的session基础数据
--author:xidongzhang@meilishuo.com
--date:2015-12-24
--======================

CREATE TABLE if not exists `tmp.tmp_collocation_samples_basedata`(
  `is_buy_query` bigint, 
  `user_id` string, 
  `twitter_id` string, 
  `real_show_nums` int, 
  `click_nums` int, 
  `real_pay_nums` int, 
  `md5query` string,
  `query` string)
PARTITIONED BY ( 
  hash_uid tinyint);


--准备session基础数据 减少二次运算成本
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict; 
SET hive.exec.max.dynamic.partitions.pernode = 2500;
SET hive.exec.max.dynamic.partitions=2000;

insert overwrite table tmp.tmp_collocation_samples_basedata partition(hash_uid)
select *
from
(select
sum(real_pay_nums) over (partition by user_id,query) as is_buy_query,
user_id,
twitter_id,
real_show_nums,
click_nums,
real_pay_nums,
md5(query) as md5query,
query,
substr(hex(md5(concat(user_id,"iahw12"))),64,1) as hash_uid
from
(select
user_id,query,twitter_id,
sum(real_show_nums) as real_show_nums,
sum(click_nums) as click_nums,
sum(real_pay_nums) as real_pay_nums
from dm.ml_base_data
where dt >'${start_dt}' and dt<'${end_dt}'
--where dt >'2015-10-01' and dt<'2015-12-25'
and user_id>0
group by user_id,query,twitter_id) tmp) t
where is_buy_query >0; -- 有购买session
