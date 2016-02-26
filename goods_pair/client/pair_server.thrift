struct PairServerRequest {
	1: string uid,							//请求的用户id
	2: i64 tid,								//请求的twitter_id
	3: string device_id,					//请求的device_id
	4: map<i16, string> model_version, 		//所使用的模型名, 及其对应的版本
											//相似模型代码: 0x01
											//搭配模型代码: 0x02
	5: optional bool recalculate,			//是否强制重新计算
	6: optional bool write_to_redis,		//是否写redis
	7: optional i16  cata_recall_num,		//搭配类目召回数目 
	8: optional i16  similar_tid_recall_num,//相似召回数
	9: optional i16  match_tid_recall_num,	//搭配召回数
	10: optional i16 similar_tid_return_num,//相似结果返回数
	11: optional i16 match_tid_return_num,	//搭配每个类目结果返回数
	12: optional list<i64> rank_tids, 		//待排序的tid集合
}

struct PairServerResponse {
	1: i16 rcode,							//返回值
	2: list<i64> similar_tids,				//相似tid集合
	3: map<string, list<i64>> match_tids	//搭配
}


service PairServer {
	i16 makePair(1:PairServerRequest request),
	PairServerResponse getPair(1:PairServerRequest request),
}
