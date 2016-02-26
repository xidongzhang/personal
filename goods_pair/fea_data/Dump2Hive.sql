--============================
--产生搭配正负样本
--流程：1.将有购买session数据写入中间数据 2.利用中间数据产生样本
--样本：1.正样本 uid1在query1下的点击购买VS.uid1在query2下的点击购买,2负样本 仅仅展现的TID.VS.购买的TID
--样本规则: if d(t1,t2)=1 then d(s(t1),s(t2))=1,d(t1,s(t2))=1,d(s(t1),t2)=1,d(t1,not(s(t1)))=0,d(not(s(t1)),t2)=0
--============================
select ${hash_uid},${hash_uid},${hash_uid},${hash_uid};
CREATE table if not exists tmp.comp_samples(
	tid1  bigint,
	tid2  bigint,
	num  int)
PARTITIONED by (
	sample_type string,
	hash_uid tinyint);

set mapred.reduce.tasks=1000;
insert overwrite table tmp.comp_samples partition(sample_type='negtive',hash_uid=${hash_uid})
select
t1.twitter_id as tid1,t2.twitter_id as tid2,sum(1) as num 
from--前session的展现无点击
(select user_id,twitter_id,md5query
	from tmp.tmp_comp_samples_basedata            
	where click_nums+real_pay_nums=0 
	and hash_uid =${hash_uid}) t1 join
--后session的购买行为
(select user_id,twitter_id,md5query
	from tmp.tmp_comp_samples_basedata
	where real_pay_nums>0 
	and hash_uid =${hash_uid}) t2  
on t1.user_id=t2.user_id 
where --同一个用户的行为,但在先后时间不同query下的有效购买session   
t1.md5query<>t2.md5query
group by t1.twitter_id,t2.twitter_id
having t1.twitter_id<>t2.twitter_id;	
--=================================================================
set mapred.reduce.tasks=1000;
insert overwrite table tmp.comp_samples partition(sample_type='positive',hash_uid=${hash_uid})
	select
	t1.twitter_id as tid1,t2.twitter_id as tid2,sum(1) as num 
	from
	(select user_id,twitter_id,md5query
		from tmp.tmp_comp_samples_basedata            
		where click_nums+real_pay_nums>0 
		and hash_uid =${hash_uid}) t1 join
	(select user_id,twitter_id,md5query
		from tmp.tmp_comp_samples_basedata            
		where click_nums+real_pay_nums>0 
		and hash_uid =${hash_uid}) t2  
	on t1.user_id=t2.user_id
	where --同一个用户的行为,但在不同query下的有效购买session   
	t1.md5query<>t2.md5query
	group by t1.twitter_id,t2.twitter_id
	having t1.twitter_id<>t2.twitter_id 

