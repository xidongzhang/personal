sql="select twitter_id from brd_goods_info where goods_status=1 and platform_type not in (3, 7)"
#sql="select twitter_id from brd_goods_info"
mysql -h10.6.7.43 -P3508 -umlsreader -p"RMlSxs&^c6OpIAQ1" -Dbrd_goods -e "${sql}" -s > brd_goods

