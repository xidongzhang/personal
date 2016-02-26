source /etc/bashrc


root_path=$1

#day=`date +"%Y-%m-%d" -d "-1 days"`
day=$2

#python getCata.py > cata_dict
#cat cata_dict | awk -F "\t" '{if($1=="家居" || $1=="美妆" ||$1=="食品")print $0}' > can_comp_cata_dict

sign=$3
sample=$4
colLimit=$5
run_pair=$6
python Main.py -root_path $root_path -day $day -sign $sign -sample $sample -colLimit $colLimit -run_pair $run_pair
#python Main.py -root_path $root_path -day $day -sign comp -sample 3 -colLimit 2 -run_pair 1
#python Main.py -root_path $root_path -day $day -sign subti -sample 30 -colLimit 3




