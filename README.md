# DyTIS: A Dynamic Dataset Targeted Index Structure Simultaneously Efficient for Search, Insert, and Scan
DyTIS (Dynamic dataset Targeted Index Structure) is an index that targets dynamic datasets. DyTIS, though based on the
structure of Extendible hashing, leverages the CDF of the key distribution of a dataset, and learns and adjusts its structure as
the dataset grows. The key novelty behind DyTIS is to group keys by the natural key order and maintain keys in sorted order
in each bucket to support scan operations within a hash index.

For more details about DyTIS, please refer to EuroSys 2023 paper (link will be uploaded).

# Prerequisites

## Software packages
```
- Ubuntu 18.04
- g++ 8.4
- libboost-dev 1.65.1 
```

- Install libboost
  ```
  sudo apt-get install libboost-all-dev
  ```



# Quick Start

## Dataset
- You can download review-small (an initial 1 million review-M) dataset from [Google Drive](https://drive.google.com/file/d/1jCJ2XSEIyUMY5tQlFIeDyKCjbbBoesDF/view?usp=sharing). 

## How to run Micro-benchmark
- The script (scripts/run_benchmark.sh) will run the experiments of insert, search, and then scan workloads over a given dataset.
- Note that a cache drop command is executed in scripts/run_benchmark.sh (Line 37). If you do not have root privileges (sudo), exclude this command.
- All throughput and latency results are saved as a log file in benchmark/result/.
  ```
  ./scripts/run_benchmark.sh [dataset path] [version (default: DTS)] [query (default: zipfian)] [insert fraction (%) (default: 50)] [range for scan (default: 100)] [log file name (optional)]
  ```
- To run benchmarks (insert, search and scan) with default settings on the review-small dataset in the path data/:

  ```
  ./scripts/run_benchmark.sh data/review-small.csv
  ```


## How to run Real-world workloads
- The script (scripts/run_ycsb_style_exp.sh) will run the experiments of seven real-world workloads that roughly
correspond to workloads Load, A, B, C, D, E, and F of YCSB over a given dataset.
- Note that a cache drop command is executed in scripts/benchmark.sh (Line 38). If you do not have root privileges (sudo), exclude this command.
- Before running the experiment, the dataset to be used in the experiment should be downloaded and located
in the proper path (e.g., in data/review-small.csv ). If the dataset is not the aforesaid one (e.g., different dataset or different path), modify dataset_files properly in the script.
- All throughput and latency results for each workload are saved as a log file in benchmark/result/.
- To run real-world workloads:

  ```
  ./scripts/run_ycsb_style_exp.sh [log file name (optional)]
  ```

