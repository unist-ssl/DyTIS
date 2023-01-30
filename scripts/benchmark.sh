#!/bin/bash
path=`pwd`/benchmark

version=$1
filepath=$2
WK=$3
bulkload_pct=$4 # a percentage of bulk loading e.g, 10 --> 10%, 5 --> 5%
query=$5
range=$6
logfile_name=$7

if [ ! -f $filepath ]
then
  echo "No such data file: "$filepath
  exit 1;
fi

# Make log file
if [ -z $logfile_name ]
then
  date_=`date '+%Y-%m-%d-%H-%M-%S'`
  logfile_name=${version}_${query}_${date_}_$WK.txt
else
  logfile_name=${logfile_name}_$WK.txt
fi

echo "logfile_name file: "$logfile_name

result_dir=$path/result
if [ ! -d $result_dir ]
then
  mkdir $result_dir
fi
logfile_name=${result_dir}/${logfile_name}

num=$(cat $filepath | wc -l)

sudo su -c "echo 3 > /proc/sys/vm/drop_caches"
echo "Drop cache"

echo "Compile "$version
make $version

echo "version: "$version "| distribution:" $query \
     "| workload:" $WK "| dataset:"  $filepath \
     "| num: " $num 2>&1 | tee -a -i $logfile_name


$path/build/benchmark \
  --workload=${WK} \
  --keys_file=${filepath} \
  --keys_file_type=text \
  --total_num_keys=$num \
  --init_num_keys=$(($num*$bulkload_pct/100)) \
  --batch_size=$(($num/10)) \
  --lookup_distribution=$query \
  --range_size=$range 2>&1 | tee -a -i $logfile_name
