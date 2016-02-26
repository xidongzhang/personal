source /etc/bashrc
basePath=$1
day=$2
sign=$3
version=$4
cur_path=`pwd`


hadoop fs -rmr $basePath/model_explain_$sign

python Main.py -model "$sign"_model_$day -fea_index fea_index_$sign -inputPath $basePath/"$sign"_train_data/round.two/final -outputPath $basePath/model_explain_$sign -sign $sign

rm model_explain_"$sign"_"$day"
hadoop fs -getmerge $basePath/model_explain_$sign model_explain_"$sign"_"$day"

#cd ../push_model
#sh push_model.sh ../model_explain/model_explain_c2_$day
cp model_explain_"$sign"_"$day" ../model_4_pair/
cd ../model_4_pair/
sh run.sh model_explain_"$sign"_"$day" model_"$sign" $sign $version


