source /etc/bashrc

modelFile=$1
tranModel=$2
sign=$3
work_dir=`pwd`

cat $modelFile | python tran.py > $tranModel
lnum=`wc -l $tranModel | awk '{print $1}'`
echo $lnum;
if [[ $lnum -lt 3000000 ]] && [[ $sign = "comp" ]]; then
#if [[ "$lnum" -lt 2500000 ]]; then
    subtitle="comp model has some problem"
    content="comp model $lnum lines has some problem"
    echo $content | mutt -s "$subtitle" chongweishen@meilishuo.com 
    echo $content | mutt -s "$subtitle" bowenzhang@meilishuo.com 
    exit -1;
fi

if [[ "$lnum" -lt 3000000 ]] && [[ $sign = "subti" ]]; then
    subtitle="subti model has some problem"
    content="subti model $lnum lines has some problem"
    echo $content | mutt -s "$subtitle" chongweishen@meilishuo.com 
    echo $content | mutt -s "$subtitle" bowenzhang@meilishuo.com 
    exit -1;
fi


if [[ $sign = "comp" ]]; then
    cp ../recall_rule/comp_cata_sim .
    python recall_rule.py $modelFile comp_cata_sim > recall_rule
    for server in `cat $work_dir/../server.list`
    do
        rsync -avz recall_rule work@$server:/home/work/service/pair_dict/output/model
    done
    #scp recall_rule
fi

for server in `cat ../server.list`
do
    rsync -avz $tranModel work@$server:/home/work/service/pair_dict/output/model
    ssh work@$server "cd /home/work/service/pair_dict; sh reload.sh"
done

subtitle="$sign model push successfully"
content="$sign model $lnum lines push successfully"
echo $content | mutt -s "$subtitle" chongweishen@meilishuo.com 
echo $content | mutt -s "$subtitle" bowenzhang@meilishuo.com 






