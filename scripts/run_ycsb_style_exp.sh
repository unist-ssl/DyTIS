#!/bin/bash


dataset_files=( 
"data/review-small.csv"
)
versions=("DTS_CUST_YCSB")
workloads=("Load" "A" "B" "C" "D" "E" "F")
queries=("zipf")
range=100
logfile_name=$1
for cnt in 1
do
  for dataset in "${dataset_files[@]}"
  do
    for version in "${versions[@]}"
    do
      for WK in "${workloads[@]}"
      do
        for query in "${queries[@]}"
        do
          echo "===> Run workload "$WK
          if [ $WK =  "Load" ]; then
            bulkload=0
            ./scripts/benchmark.sh $version $dataset $WK $bulkload $query $range $logfile_name
          elif [ $WK =  "D" ] || [ $WK = E ]; then
            bulkload=80
            ./scripts/benchmark.sh $version $dataset $WK $bulkload $query $range $logfile_name
          else # For A, B, C and F 
            bulkload=100
            ./scripts/benchmark.sh $version $dataset $WK $bulkload $query $range $logfile_name
          fi
        done
      done
      sleep 10
    done
  done
done
