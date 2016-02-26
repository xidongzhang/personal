#!/bin/bash

python get_data.py 5 28 train
#
$HADOOP_HOME/bin/hadoop fs -rmr ./training_data/round.zero*
#round 0 : merge data with position bias
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
-input /user/hive/warehouse/tmp.db/temp_show_click_goods_info_train/*_0.lzo_deflate \
-output ./training_data/round.zero -mapper ./map.py -reducer ./red.py -file ./map.py -file ./red.py -file ./position_bias.dict  -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400 \

$HADOOP_HOME/bin/hadoop fs -rmr ./training_data/round.one*
#round 1 : generate data
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
-input ./training_data/round.zero \
-output ./training_data/round.one -mapper ./round1.map.py -reducer cat -file ./round1.map.py -file ./discretization.config -file ./position_bias.dict  -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400 \

$HADOOP_HOME/bin/hadoop fs -rmr ./training_data/round.two*
#round 2 : prepare data
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
 -input ./training_data/round.one/part-* \
 -output ./training_data/round.two \
 -mapper ./round2.map.py -reducer ./round2.red.py \
 -file ./round2.map.py -file ./round2.red.py -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400

rm ./round2.tmp
hadoop fs -rm ./training_data/fea2id.txt
hadoop fs -getmerge ./training_data/round.two ./round2.tmp
cat ./round2.tmp | python ./round2.no.py > fea2id.txt
hadoop fs -copyFromLocal fea2id.txt ./training_data/


hadoop fs -rmr ./training_data/round.three
#round 3: 
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
 -input ./training_data/round.one/part-* -input ./training_data/fea2id.txt \
 -output ./training_data/round.three \
 -mapper ./round3.map.py -file ./round3.map.py \
 -jobconf stream.num.map.output.key.fields=2 \
 -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
 -jobconf num.key.fields.for.partition=1 \
 -jobconf mapred.reduce.tasks=400 \
 -reducer ./round3.red.py -file ./round3.red.py

hadoop fs -rmr ./training_data/round.four
#round 4: 
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
 -input ./training_data/round.one/part-* -input ./training_data/round.three/part-* \
 -output ./training_data/round.four \
 -mapper ./round4.map.py -file ./round4.map.py \
 -jobconf stream.num.map.output.key.fields=2 \
 -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
 -jobconf num.key.fields.for.partition=1 \
 -jobconf mapred.reduce.tasks=400 \
 -reducer ./round4.red.py -file ./round4.red.py

rm training.data
hadoop fs -getmerge ./training_data/round.four ./training.data


##############################################
# testing data
##############################################

python get_data.py 4 1 testing
#
$HADOOP_HOME/bin/hadoop fs -rmr ./testing_data/round.zero*
#round 0 : merge data with position bias
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
-input /user/hive/warehouse/tmp.db/temp_show_click_goods_info_testing/*_0.lzo_deflate \
-output ./testing_data/round.zero -mapper ./map.py -reducer ./red.py -file ./map.py -file ./red.py -file ./position_bias.dict  -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400 \

$HADOOP_HOME/bin/hadoop fs -rmr ./testing_data/round.one*
#round 1 : generate data
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
-input ./testing_data/round.zero \
-output ./testing_data/round.one -mapper ./round1.map.py -reducer cat -file ./round1.map.py -file ./discretization.config -file ./position_bias.dict  -jobconf mapred.map.tasks=400 -jobconf mapred.reduce.tasks=400 \

hadoop fs -rmr ./testing_data/round.three
#round 3: 
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
 -input ./testing_data/round.one/part-* -input ./training_data/fea2id.txt \
 -output ./testing_data/round.three \
 -mapper ./round3.map.py -file ./round3.map.py \
 -jobconf stream.num.map.output.key.fields=2 \
 -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
 -jobconf num.key.fields.for.partition=1 \
 -jobconf mapred.reduce.tasks=400 \
 -reducer ./round3.red.py -file ./round3.red.py

hadoop fs -rmr ./testing_data/round.four
#round 4: 
hadoop jar /hadoop/hadoop/contrib/streaming/hadoop-streaming-1.2.2-SNAPSHOT.jar \
 -input ./testing_data/round.one/part-* -input ./testing_data/round.three/part-* \
 -output ./testing_data/round.four \
 -mapper ./round4.map.py -file ./round4.map.py \
 -jobconf stream.num.map.output.key.fields=2 \
 -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
 -jobconf num.key.fields.for.partition=1 \
 -jobconf mapred.reduce.tasks=400 \
 -reducer ./round4.red.py -file ./round4.red.py

rm testing.data

hadoop fs -getmerge ./testing_data/round.four testing.data

cd ..
sh -x do.train.eval.sh ./get_train_data/training.data ./get_train_data/testing.data
