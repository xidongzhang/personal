source /etc/bashrc

work_dir=`pwd`

lnum=`wc -l forward_index | awk '{print $1}'`
echo $lnum;
if [[ $lnum -lt 6000000 ]]; then
    subtitle="forward index has some problem"
    content="forward index $lnum lines has some problem"
    echo $content | mutt -s "$subtitle" chongweishen@meilishuo.com
    echo $content | mutt -s "$subtitle" bowenzhang@meilishuo.com
    exit -1;
fi

invert_num=`wc -l inverted_index | awk '{print $1}'`
echo $invert_num;
if [[ $invert_num -lt 200000 ]]; then
    subtitle="inverted index has some problem"
    content="inverted index $invert_num lines has some problem"
    echo $content | mutt -s "$subtitle" chongweishen@meilishuo.com
    echo $content | mutt -s "$subtitle" bowenzhang@meilishuo.com
    exit -1;
fi


for server in `cat ../server.list`
do
    rsync -avz forward_index work@$server:/home/work/service/pair_dict/output/dict
    rsync -avz inverted_index work@$server:/home/work/service/pair_dict/output/dict
    rsync -avz direct_index work@$server:/home/work/service/pair_dict/output/dict
done

subtitle="indexes push successfully"
content="indexes forward $lnum lines, invert $invert_num lines push successfully"
echo $content | mutt -s "$subtitle" chongweishen@meilishuo.com 
echo $content | mutt -s "$subtitle" bowenzhang@meilishuo.com 






