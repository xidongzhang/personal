#!/usr/bin/python

import sys
import os
import time
import datetime


sep1=','
sep2="'"
thedays='('
end=')'
now=datetime.datetime.now()
i = int(sys.argv[1])
while i < int(sys.argv[2]):
    	delta = datetime.timedelta(days=i)     
    	n_days = now - delta
    	theday =  n_days.strftime('%Y-%m-%d')
	thedays = thedays+sep2+theday+sep2+sep1
	i += 1
delta=datetime.timedelta(days=i)
n_days = now - delta
theday =  n_days.strftime('%Y-%m-%d')
thedays = thedays+sep2+theday+sep2+end

print 'need days:', thedays

hql='''
	hive -e "
        use tmp;
        drop table if exists ml_show_click_goods_info_%s;
        set mapred.reduce.tasks = 400;
        create table ml_show_click_goods_info_%s as
        select u3.show_nums show_nums, u3.click_nums click_nums, u3.pos pos, u3.poster_word poster_word, u4.goods_id goods_id, u4.twitter_id twitter_id, u4.shop_id shop_id, u4.title title, u4.tag tag, u4.goods_price goods_price, u4.discount_off discount_off, u4.goods_ctime goods_ctime, u4.goods_status goods_status, u4.goods_color goods_color, u4.goods_size goods_size, u4.goods_detail_cata goods_detail_cata, u4.goods_style goods_style, u4.gmv_score gmv_score, u4.discount_price_14 discount_price_14, u4.pic_score pic_score, u4.style_score style_score, u4.rank_like rank_like, u4.sale_num sale_num, u4.goods_img goods_img, u4.goods_on_shelf goods_on_shelf, u4.shop_ctime shop_ctime, u4.goods_satisfied goods_satisfied, u4.goods_dsr goods_dsr, u4.goods_fashion goods_fashion, u4.goods_quality goods_quality, u4.reason_refund_rate reason_refund_rate, u4.appeal_rate appeal_rate, u4.comment_num comment_num, u4.goods_repertory goods_repertory, u4.shop_salenum shop_salenum, u4.shop_total_subscriber shop_total_subscriber, u4.shop_online_sku shop_online_sku, u4.shop_fashion shop_fashion, u4.shop_quality shop_quality, u4.shop_attitude shop_attitude, u4.shop_fast shop_fast, u4.discount_rage discount_rage  from (select u2.twitter_id twitter_id, u2.poster_word poster_word, u2.pos pos, u2.show_nums show_nums, u2.click_nums click_nums   from (select u1.twitter_id twitter_id, u1.poster_word poster_word, u1.pos pos, sum(u1.show_nums) show_nums, sum(u1.click_nums) click_nums from tmp.ml_show_click_table u1  where u1.dt in %s and u1.show_nums>0 and u1.pos<150  group by u1.twitter_id, u1.poster_word, u1.pos) u2 where u2.show_nums>4 or u2.click_nums > 0) u3 join tmp.scw_goods_detail_info u4 on (u3.twitter_id=u4.twitter_id);"
        '''

hql=hql%(sys.argv[3],sys.argv[3],thedays)

print hql
os.system(hql)

