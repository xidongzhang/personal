day=`date +"%Y%m%d" -d "0 days"`

scp ml@bj-pfredis05:/home/ml/chongweishen/code/rank_ml/l1_model_1_new .
hadoop fs -rmr /user/chongweishen/personal/rank/ctr_study_train/model_explain

python Main.py -model l1_model_1_new -fea_index fea_index -inputPath /user/ml/personal_dev_new/training_data/round.two/final -outputPath /user/chongweishen/personal/rank/ctr_study_train/model_explain

hadoop fs -getmerge /user/chongweishen/personal/rank/ctr_study_train/model_explain model_explain



