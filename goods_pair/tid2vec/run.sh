source /etc/bashrc

work_dir=`pwd`

cd $work_dir/action_seqence;
sh run.sh
cd $work_dir
hadoop fs -getmerge /user/ml/personal/word2vec/action_seq action_seq
mv action_seq $work_dir/word2vec

cd $work_dir/word2vec
sh seq_tid.sh 
hadoop fs -put action_seq_model /user/ml/personal/word2vec/action_seq_model

