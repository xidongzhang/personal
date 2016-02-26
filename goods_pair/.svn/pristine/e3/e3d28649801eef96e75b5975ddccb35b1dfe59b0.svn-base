#!/bin/bash

HDFS_HOME="/user/chongweishen/recommend/pair_rec"
#sign1="subti"
#sign2="train"
sample=100
rowLimit=1

HDFS_HOME=$1
sign1=$2
sign2=$3

FEA_CONFIG="./discretization.config"

#train=$sign
#train="training"
#evaluation="testing"

fea_optimization=0

# feature discretization
# $1 : training
round_two_mapred ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_$2_data/round.two
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/fea_data/$1_comb_fea \
    -output $HDFS_HOME/$1_$2_data/round.two \
    -mapper ./round2.map1.py -file ./round2.map1.py \
    -jobconf mapred.map.tasks=1200 -jobconf mapred.reduce.tasks=0 \
	-jobconf mapred.output.compress=true

    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/$1_$2_data/round.two/part-* \
    -output $HDFS_HOME/$1_$2_data/round.two/cl \
    -mapper ./round2.countlines.py -reducer 'cat' \
    -file ./round2.countlines.py \
    -jobconf mapred.map.tasks=1200

	if [ -f ./round2_$1.cl ]; then
		rm ./round2_$1.cl
	fi

	$HADOOP_HOME/bin/hadoop fs -getmerge $HDFS_HOME/$1_$2_data/round.two/cl ./round2_$1.cl

	param=`cat round2_$1.cl | python round2.compute.py`

    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/$1_$2_data/round.two/part-* \
    -output $HDFS_HOME/$1_$2_data/round.two/final \
    -mapper ./round2.assign.py -reducer NONE \
    -file ./round2.assign.py \
    -jobconf mapred.map.tasks=1200 \
	-cmdenv "P=$param"

}
    

# allocate number for every feature
# $1 : training
round_three_mapred ()
{    
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_$2_data/round.three
    #round 3: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/fea_data/$1_comb_fea -input $HDFS_HOME/$1_$2_data/round.two/final/part-* \
    -output $HDFS_HOME/$1_$2_data/round.three \
    -mapper ./round3.map.py -file ./round3.map.py \
    -jobconf stream.num.map.output.key.fields=2 \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf num.key.fields.for.partition=1 \
    -jobconf mapred.reduce.tasks=2489 \
    -reducer ./round3.red.py -file ./round3.red.py
}



# get samples with discreted features
# $1 : training | testing
round_four ()
{
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_$2_data/round.four
    #round 4: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -input $HDFS_HOME/fea_data/$1_comb_fea -input $HDFS_HOME/$1_$2_data/round.three/part-* \
    -output $HDFS_HOME/$1_$2_data/round.four \
    -mapper ./round4.map.py -file ./round4.map.py \
    -jobconf mapred.reduce.tasks=2480 \
    -reducer ./round4.red.py -file ./round4.red.py
    
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
    $HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_$2_data/round.five
    #round 4: 
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
    -D mapred.reduce.tasks=1419 \
    -input $HDFS_HOME/$1_$2_data/round.four/part-* \
    -output $HDFS_HOME/$1_$2_data/round.five \
    -mapper ./round5.map.py -file ./round5.map.py \
    -reducer "./round5.red.py $rowLimit $sample" -file ./round5.red.py \
    
    if [ -f ./$1.data.merge ]; then
        rm ./$1.data.merge
    fi
    #-outputformat scw.meilishuo.hadoop.com.cn.ScwMultipleOutputFormat \
    #-libjars "./ScwMultipleOutputFormat_fat.jar," -input $HDFS_HOME/$1_data/round.four/part-* \

    #$HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/for_train;
    #$HADOOP_HOME/bin/hadoop fs -rmr $HDFS_HOME/$1_data/for_test;
    #$HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_HOME/$1_data/for_train;
    #$HADOOP_HOME/bin/hadoop fs -mkdir $HDFS_HOME/$1_data/for_test;

    #$HADOOP_HOME/bin/hadoop fs -mv $HDFS_HOME/$1_data/round.five/part-*-A $HDFS_HOME/$1_data/for_train/
    #$HADOOP_HOME/bin/hadoop fs -mv $HDFS_HOME/$1_data/round.five/part-*-B $HDFS_HOME/$1_data/for_test/

    #$HADOOP_HOME/bin/hadoop fs -getmerge $HDFS_HOME/$1_data/round.five ./$1.data.merge
}


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

round_two_mapred $sign1 $sign2
round_three_mapred $sign1 $sign2
round_four $sign1 $sign2
round_five $sign1 $sign2

##############################################
# testing data
##############################################

#if [ $fea_optimization -ne 1 ]; then
#    python get_data.py 1 1 $evaluation
#    round_zero $evaluation
#fi

#round_one $evaluation
#round_three $evaluation
#round_four_4_test $evaluation

#cd ..
#sh -x do.train.eval.sh ./get_train_data/$train.data ./get_train_data/$evaluation.data $train"fea2id.txt"

echo "Finished"
