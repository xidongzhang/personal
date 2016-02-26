
dataPath=$1
day=$2

#mkdir bak/$yestoday
#mv *$yestoday bak/$yestoday/

rm training_data_subti
/hadoop/hadoop/bin/hadoop fs -getmerge $dataPath training_data_subti
./bin/train -c 4 -e 0.004 training_data_subti subti_model_$day



