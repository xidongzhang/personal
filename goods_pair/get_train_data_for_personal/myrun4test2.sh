#!/bin/bash

HDFS_HOME="/user/ml/personal_dev_new_2"
FEA_CONFIG="./discretization.config"

train="training"
evaluation="testing"

fea_optimization=0

dep1=/home/ml/pest/init_show_click/
dep2=/home/ml/pest/get_goods_info/

#round 0 : merge data with position bias
# $1 : training | testing
round_zero ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.zero*
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input /user/hive/warehouse/tmp.db/ml_show_click_goods_info_$1/*_0.lzo_deflate \
    -output $HDFS_HOME/$1_data/round.zero \
    -mapper ./map.py -reducer ./red.py -file ./map.py -file ./red.py -file ./position_bias.dict \
    -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400
}

# featue extraction
# $1 : training | testing
round_one () 
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.one*
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/$1_data/round.zero \
    -output $HDFS_HOME/$1_data/round.one \
    -mapper ./round1.map.py -reducer NONE \
    -file ./round1.map.py -file $FEA_CONFIG -file ./discretization.config \
    -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400
}

# featue extraction
# $1 : training | testing
round_one_with_filter () 
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.one*
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/$1_data/round.zero \
    -output $HDFS_HOME/$1_data/round.one \
    -mapper ./round1.map.withfilter.py -reducer round1.reduce.withfilter.py \
    -file ./round1.map.withfilter.py -file round1.reduce.withfilter.py -file $FEA_CONFIG -file ./discretization.config \
    -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400
}

# feature discretization
# $1 : training
round_two ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr ./$1_data/round.two*
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/$1_data/round.one/part-* \
    -output $HDFS_HOME/$1_data/round.two \
    -mapper ./round2.map.py -reducer ./round2.red.py \
    -file ./round2.map.py -file ./round2.red.py \
    -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400
    
    if [ -f ./round2.tmp ]; then
        rm ./round2.tmp
    fi

    $HADOOP_HOME/bin/hadoop fs -rm $HDFS_HOME/$1_data/fea2id.txt
    $HADOOP_HOME/bin/hadoop fs -getmerge $HDFS_HOME/$1_data/round.two ./round2.tmp
    cat ./round2.tmp | python ./round2.no.py > $1fea2id.txt
    hadoop fs -copyFromLocal $1fea2id.txt $HDFS_HOME/$1_data/fea2id.txt
}

# feature discretization
# $1 : training
round_two_mapred ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.two
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input /user/chongweishen/personal/ctr_common/data_filt \
    -output $HDFS_HOME/$1_data/round.two \
    -mapper ./round2.map1.py -reducer ./round2.red2.py \
    -file ./round2.map1.py -file ./round2.red2.py\
    -jobconf mapred.map.tasks=1200 -jobconf mapred.reduce.tasks=1697 \
	-jobconf mapred.output.compress=true

    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/$1_data/round.two/part-* \
    -output $HDFS_HOME/$1_data/round.two/cl \
    -mapper ./round2.countlines.py -reducer 'cat' \
    -file ./round2.countlines.py \
    -jobconf mapred.map.tasks=1200

	if [ -f ./round2.cl ]; then
		rm ./round2.cl
	fi

	$HADOOP_HOME/bin/hadoop fs -getmerge $HDFS_HOME/$1_data/round.two/cl ./round2.cl

	param=`cat round2.cl | python round2.compute.py`

    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/$1_data/round.two/part-* \
    -output $HDFS_HOME/$1_data/round.two/final \
    -mapper ./round2.assign.py -reducer NONE \
    -file ./round2.assign.py \
    -jobconf mapred.map.tasks=1200 \
	-cmdenv "P=$param"

}
    
# allocate number for every feature
# $1 : training
round_three ()
{    
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.three
    #round 3: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input /user/chongweishen/personal/rank/ctr_study_test2/data_filt -input /user/ml/personal_dev_new/training_data/round.two/final/* \
    -output $HDFS_HOME/$1_data/round.three \
    -mapper ./round3.map.py -file ./round3.map.py \
    -jobconf stream.num.map.output.key.fields=2 \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf num.key.fields.for.partition=1 \
    -jobconf mapred.reduce.tasks=2400 \
    -reducer ./round3.red.py -file ./round3.red.py
}

# allocate number for every feature
# $1 : training
round_three_mapred ()
{    
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.three
    #round 3: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input /user/chongweishen/personal/ctr_common/data_filt -input $HDFS_HOME/$1_data/round.two/final/part-* \
    -output $HDFS_HOME/$1_data/round.three \
    -mapper ./round3.map.py -file ./round3.map.py \
    -jobconf stream.num.map.output.key.fields=2 \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf num.key.fields.for.partition=1 \
    -jobconf mapred.reduce.tasks=4489 \
    -reducer ./round3.red.py -file ./round3.red.py
}



# get samples with discreted features
# $1 : training | testing
round_four ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.four
    #round 4: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input /user/chongweishen/personal/ctr_common/data_filt -input $HDFS_HOME/$1_data/round.three/part-* \
    -output $HDFS_HOME/$1_data/round.four \
    -mapper ./round4.map.py -file ./round4.map.py \
    -jobconf mapred.reduce.tasks=5480 \
    -reducer ./round4.red.py -file ./round4.red.py
    
    if [ -f ./$1.data ]; then
        rm ./$1.data
    fi

    #-jobconf stream.num.map.output.key.fields=2 \
    #-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    #-jobconf num.key.fields.for.partition=1 \
    
    #$HADOOP_HOME/bin/hadoop fs -getmerge $HDFS_HOME/$1_data/round.four ./$1.data
}

round_four_4_test ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.four
    #round 4: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -jobconf stream.num.map.output.key.fields=2 \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf num.key.fields.for.partition=1 \
    -input /user/chongweishen/personal/rank/ctr_study_test2/data_filt -input $HDFS_HOME/$1_data/round.three/part-* \
    -output $HDFS_HOME/$1_data/round.four \
    -mapper ./round4.test.map.py -file ./round4.test.map.py \
    -jobconf mapred.reduce.tasks=3480 \
    -reducer ./round4.test.red.py -file ./round4.test.red.py
    
    if [ -f ./$1.data ]; then
        rm ./$1.data
    fi

    #-jobconf stream.num.map.output.key.fields=2 \
    #-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    #-jobconf num.key.fields.for.partition=1 \
    
    #$HADOOP_HOME/bin/hadoop fs -getmerge $HDFS_HOME/$1_data/round.four ./$1.data
}

# merge data
# $1 : training | testing
round_five ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/round.five
    #round 4: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -D mapred.reduce.tasks=2419 \
    -libjars "./ScwMultipleOutputFormat_fat.jar," -input $HDFS_HOME/$1_data/round.four/part-* \
    -output $HDFS_HOME/$1_data/round.five \
    -outputformat scw.meilishuo.hadoop.com.cn.ScwMultipleOutputFormat \
    -mapper ./round5.map.py -file ./round5.map.py \
    -reducer ./round5.red.py -file ./round5.red.py \
    
    if [ -f ./$1.data.merge ]; then
        rm ./$1.data.merge
    fi

    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/for_train;
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/for_test;
    $HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_HOME/$1_data/for_train;
    $HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_HOME/$1_data/for_test;

    $HADOOP_HOME/bin/hadoop fs -mv $HDFS_HOME/$1_data/round.five/part-*-A $HDFS_HOME/$1_data/for_train/
    $HADOOP_HOME/bin/hadoop fs -mv $HDFS_HOME/$1_data/round.five/part-*-B $HDFS_HOME/$1_data/for_test/

    #$HADOOP_HOME/bin/hadoop fs -getmerge $HDFS_HOME/$1_data/round.five ./$1.data.merge
}

waiting_dependency ()
{
	donefile=`date -d "-1 day" +%Y%m%d`

	finish=0
	while [ $finish != 1 ]
	do
		if [ -f $dep1$donefile".done" ] && [ -f $dep2$donefile".done" ] ; then
			finish=1
		else
			echo `date`":"$dep1$donefile".done or "$dep2$donefile".done not ready. sleep 1minutes"
			sleep 1m
		fi
	done
	
}


##############################################
# make sure dependency data is ready
##############################################
#waiting_dependency

##############################################
# training data
##############################################

# prepare basic feature data and sample count
#if [ $fea_optimization -ne 1 ]; then
#    python get_data.py 1 28 $train
#    round_zero $train
#fi
#round_one $train
#round_two $train
#round_three $train
#round_four $train

#round_one_with_filter $train

#round_two_mapred $train
#round_three_mapred $train
#round_four $train
#round_five $train

##############################################
# testing data
##############################################

#if [ $fea_optimization -ne 1 ]; then
#    python get_data.py 1 1 $evaluation
#    round_zero $evaluation
#fi

#round_one $evaluation
round_three $evaluation
round_four_4_test $evaluation

#cd ..
#sh -x do.train.eval.sh ./get_train_data/$train.data ./get_train_data/$evaluation.data $train"fea2id.txt"

echo "Finished"
