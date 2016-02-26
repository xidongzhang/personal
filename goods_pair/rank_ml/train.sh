
dataPath=$1
day=$2
sign=$3

#mkdir bak/$yestoday
#mv *$yestoday bak/$yestoday/

rm training_data_$sign
/hadoop/hadoop/bin/hadoop fs -getmerge $dataPath training_data_$sign
./bin/train -c 4 -e 0.004 training_data_$sign $3_model_$day



