--============================
--产生搭配正负样本
--流程：1.将有购买session数据写入中间数据 2.利用中间数据产生样本
--样本：1.正样本 uid1在query1下的点击购买VS.uid1在query2下的点击购买,2负样本 仅仅展现的TID.VS.购买的TID
--样本规则: if d(t1,t2)=1 then d(s(t1),s(t2))=1,d(t1,s(t2))=1,d(s(t1),t2)=1,d(t1,not(s(t1)))=0,d(not(s(t1)),t2)=0
--author:xidongzhang@meilishuo.com
--date:2015-12-24
--============================

CREATE table if not exists tmp.collocations_samples(
	tid1  bigint,
	tid2  bigint,
	num  int)
PARTITIONED by (sample_type string);

--Asession的点击购买行为 VS Bsession的点击购买行为组成正样本
insert into table tmp.collocations_samples partition(sample_type)
select
tid1,tid2,sum(num) as num,'positive' as sample_type
from
(select
	t1.dt,t1.twitter_id as tid1,t2.twitter_id as tid2,sum(1) as num
	from--前session的点击、购买
	(select dt,user_id,twitter_id,query
		from tmp.tmp_collocation_samples_basedata
		where dt>'${start_dt}' and dt <'${end_dt}'
		and click_nums+real_pay_nums>0) t1 join
	--后session的点击、购买
	(select dt,user_id,twitter_id,query
		from tmp.tmp_collocation_samples_basedata
		where dt>'${start_dt}' and dt <'${end_dt}'
		and click_nums+real_pay_nums>0 )t2
	on t1.user_id=t2.user_id 
	where --同一个用户的行为,但在先后时间不同query下的有效购买session
	t1.dt < t2.dt and t2.dt <date_add(t1.dt,8)
	and t1.query<>t2.query
	group by t1.dt,t1.twitter_id,t2.twitter_id
	having t1.twitter_id<>t2.twitter_id) tmp
group by tid1,tid2;
--============================================================
--负样本
--============================================================
--A session的展现无点击行为 VS B session的购买行为组成负样本
insert into table tmp.collocations_samples partition(sample_type)
select tid1,tid2,sum(num) as num,'negative' as sample_type
from 
(select
	t1.dt,t1.twitter_id as tid1,t2.twitter_id as tid2,sum(1) as num
	from--前session的展现无点击
	(select dt,user_id,twitter_id,query
		from tmp.tmp_collocation_samples_basedata
		where dt>'${start_dt}' and dt <'${end_dt}'
		and click_nums+real_pay_nums=0) t1 join
	--后session的购买行为
	(select dt,user_id,twitter_id,query
		from tmp.tmp_collocation_samples_basedata
		where dt>'${start_dt}' and dt <'${end_dt}'
		and real_pay_nums>0) t2 on t1.user_id=t2.user_id 
	where --同一个用户的行为,但在先后时间不同query下的有效购买session
	t1.dt < t2.dt and t2.dt <date_add(t1.dt,8)
	and t1.query<>t2.query
	group by t1.dt,t1.twitter_id,t2.twitter_id
	having t1.twitter_id<>t2.twitter_id) tmp
group by tid1,tid2

