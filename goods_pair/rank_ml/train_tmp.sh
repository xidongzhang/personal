
#basePath=$1
#today=$2
#yestoday=$3
#sign=$4
hdfsData="/user/chongweishen/recommend/pair_rec/comp_train_data/round.five"

#mkdir bak/$yestoday
#mv *$yestoday bak/$yestoday/

rm training_data
/hadoop/hadoop/bin/hadoop fs -getmerge $hdfsData training_data
./bin/train -c 2 -e 0.001 training_data comp_model



