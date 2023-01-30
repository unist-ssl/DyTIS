#!/bin/bash
path=`pwd`/benchmark

keyfile_path=$1
version=${2:-"DTS"}
query=${3:-"zipf"}
insert=${4:-50}
range=${5:-100}
logfile_name=$6

if [ -z $keyfile_path ] ; then
  echo "[USAGE] [key file path] [version (default: DTS)] [query (default: zipfian)] [insert fraction (default: 50 %)] [range for scan (default: 100)] [log file name (optional)]"
  exit 1
fi
if [ ! -f $keyfile_path ] ; then
  echo "No such key file path: "$keyfile_path
  exit 1
fi

# Make log file
if [ -z $logfile_name ] ; then
  date_=`date '+%Y-%m-%d-%H-%M-%S'`
  logfile_name=${version}_${query}_${date_}.txt
else
  logfile_name=${logfile_name}.txt
fi

result_dir=$path/result
if [ ! -d $result_dir ] ; then
  mkdir $result_dir
fi
logfile_name=${result_dir}/${logfile_name}
echo "logfile_name file: "$logfile_name

num=$(cat $keyfile_path | wc -l)

sudo su -c "echo 3 > /proc/sys/vm/drop_caches"
echo "Drop cache"

echo "Compile "$version
make $version

echo "version: "$version "| distribution:" $query \
     "| insert frac:" $insert "| workload:"  $keyfile_path \
     "| num: " $num 2>&1 | tee -a -i $logfile_name
insert_frac=`awk "BEGIN {print $insert/100}"`
scale_factor=$((100/$insert))

$path/build/benchmark \
--keys_file=${keyfile_path} \
--keys_file_type=text \
--total_num_keys=$num \
--batch_size=$(($num*$scale_factor)) \
--insert_frac=$insert_frac \
--lookup_distribution=$query \
--range_size=$range 2>&1 | tee -a -i $logfile_name
