/*
Copyright 2023, The DyTIS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code modified from https://github.com/microsoft/ALEX/tree/57efb5005bd0e769ecee9c2bc75125f8ea340730/src/benchmark
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <cassert>
#include <iomanip>
#include <vector>
#include <thread>
#include <algorithm>
#include <stdlib.h>
#include <unistd.h>
#include "util/pair.h"
#include <string>
#include <sstream>
#include <stdlib.h>
#include <numeric>

#include "flags.h"
#include "utils.h"

#include "src/DyTIS.h"
#include "src/DyTIS_impl.h"

#define KEY_TYPE uint64_t
#define PAYLOAD_TYPE uint64_t

using namespace std;

/*
 * Required flags:
 * --keys_file              path to the file that contains keys
 * --keys_file_type         file type of keys_file (options: binary or text)
 * --total_num_keys         total number of keys in the keys file
 * --batch_size             number of operations (lookup or insert) per batch
 *
 * Optional flags:
 * --insert_frac            fraction of operations that are inserts (instead of
 * lookups)
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  std::string keys_file_type = get_required(flags, "keys_file_type");
  auto total_num_keys = stoi(get_required(flags, "total_num_keys"));
  auto batch_size = stoi(get_required(flags, "batch_size"));
  auto insert_frac = stod(get_with_default(flags, "insert_frac", "0.5"));
  std::string lookup_distribution =
      get_with_default(flags, "lookup_distribution", "zipf");
  auto time_limit = stod(get_with_default(flags, "time_limit", "1.0"));
  bool print_batch_stats = get_boolean_flag(flags, "print_batch_stats");
  auto range_size = stoi(get_required(flags, "range_size"));

  const size_t kInitialTableSize = 16*1024;

  // Read keys from file
  std::cout << "Start reading keys from file" << std::endl;
  auto keys = new KEY_TYPE[total_num_keys];
  if (keys_file_type == "binary") {
    load_binary_data(keys, total_num_keys, keys_file_path);
  } else if (keys_file_type == "text") {
    load_text_data(keys, total_num_keys, keys_file_path);
  } else {
    std::cerr << "--keys_file_type must be either 'binary' or 'text'"
              << std::endl;
    return 1;
  }
  std::cout << "Finish reading keys from file" << std::endl;

  std::mt19937_64 gen_payload(std::random_device{}());
  DyTIS* index = new DyTIS();

  // Run workload
  int i = 0;
  long long cumulative_inserts = 0;
  long long cumulative_lookups = 0;
  long long cumulative_scans = 0;
  int num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
  int num_lookups_per_batch = batch_size - num_inserts_per_batch;
  // TODO: Find an appropriate number
  int num_scans_per_batch = num_lookups_per_batch / range_size;

  double cumulative_insert_time = 0;
  double cumulative_lookup_time = 0;
  double cumulative_scan_time = 0;
  std::cout << "num_inserts_per_batch: " << num_inserts_per_batch << std::endl;
  std::cout << "num_lookups_per_batch: " << num_lookups_per_batch << std::endl;
  std::cout << "num_scans_per_batch: " << num_scans_per_batch << std::endl;

  int batch_no = 0;
  std::cout << std::scientific;
  std::cout << std::setprecision(3);
  batch_no++;


  int num_actual_inserts =
      std::min(num_inserts_per_batch, total_num_keys - i);
  int num_keys_after_batch = i + num_actual_inserts;
  std::cout << "[batch_no: " << batch_no << "] "
            << "num_actual_inserts: " << num_actual_inserts << std::endl;
  std::cout << "[batch_no: " << batch_no << "] "
            << "num_keys_after_batch: " << num_keys_after_batch << std::endl;


  // Do inserts
  std::cout << "insert start!" << std::endl;
  auto inserts_start_time = std::chrono::high_resolution_clock::now();
  for (; i < num_keys_after_batch; i++) {
    index->Insert(keys[i], static_cast<PAYLOAD_TYPE>(gen_payload()));
  }
  auto inserts_end_time = std::chrono::high_resolution_clock::now();
  double batch_insert_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(inserts_end_time -
                                                           inserts_start_time)
          .count();
  cumulative_insert_time += batch_insert_time;
  cumulative_inserts += num_actual_inserts;
  std::cout << "insert finish!" << std::endl;
  std::cout << "Cumulative stats: " << batch_no << " batches, "
            << cumulative_inserts << " inserts"
            << "\n----------------------------------------------------------"
            << "\n\tcumulative insert throughput:\t"
            << cumulative_inserts / cumulative_insert_time * 1e9
            << " inserts/sec,\t"
            << "\n----------------------------------------------------------"
            << std::endl;

  // Do lookups
  KEY_TYPE* lookup_keys = nullptr;
  if (lookup_distribution == "uniform") {
    lookup_keys = get_search_keys(keys, i, num_lookups_per_batch);
  } else if (lookup_distribution == "zipf") {
    lookup_keys = get_search_keys_zipf(keys, i, num_lookups_per_batch);
  } else {
    std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'"
              << std::endl;
    return 1;
  }

  std::cout << "lookup start!" << std::endl;
  auto lookups_start_time = std::chrono::high_resolution_clock::now();
  for (int j = 0; j < num_lookups_per_batch; j++) {
    KEY_TYPE key = lookup_keys[j];
    Value_t ret = index->Get(key);
  }
  auto lookups_end_time = std::chrono::high_resolution_clock::now();
  double batch_lookup_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(lookups_end_time -
                                                           lookups_start_time)
          .count();


  delete[] lookup_keys;
  cumulative_lookup_time += batch_lookup_time;
  cumulative_lookups += num_lookups_per_batch;
  std::cout << "lookup finish!" << std::endl;
  std::cout << "Cumulative stats: " << batch_no << " batches, "
            << cumulative_lookups << " lookups"
            << "\n----------------------------------------------------------"
            << "\n\tcumulative lookup throughput:\t"
            << cumulative_lookups / cumulative_lookup_time * 1e9
            << " lookups/sec,\t"
            << "\n----------------------------------------------------------"
            << std::endl;

  // Do scans
  std::cout << "scan start!" << std::endl;

  auto time_scan_start = std::chrono::high_resolution_clock::now();
  while (1) {
    KEY_TYPE* scan_start_keys = nullptr;
    scan_start_keys = get_search_keys_zipf(keys, i, num_scans_per_batch);

    auto scan_start_time = std::chrono::high_resolution_clock::now();
    for (int j = 0; j < num_scans_per_batch; j++) {
      KEY_TYPE key = scan_start_keys[j];
      auto payload = index->Scan(key, range_size);
      delete[] payload;
    }

    auto scan_end_time = std::chrono::high_resolution_clock::now();
    double batch_scan_time =
        std::chrono::duration_cast<std::chrono::nanoseconds>(scan_end_time -
                                                             scan_start_time)
            .count();
    cumulative_scan_time += batch_scan_time;
    cumulative_scans += num_scans_per_batch;
    double workload_elapsed_time =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::high_resolution_clock::now() - time_scan_start)
            .count();
    delete[] scan_start_keys;
    if (workload_elapsed_time > time_limit * 1e9 *60) {
      break;
    }
  }

  std::cout << "scan finish!" << std::endl;

  // Check for workload end conditions
  std::cout << "num_actual_inserts: "
            << num_actual_inserts << std::endl;
  std::cout << "num_inserts_per_batch: "
            << num_inserts_per_batch << std::endl;

  long long cumulative_operations = cumulative_lookups + cumulative_inserts + cumulative_scans;
  double cumulative_time = cumulative_lookup_time + cumulative_insert_time + cumulative_scan_time;
  std::cout << "Cumulative stats: " << batch_no << " batches, "
            << cumulative_operations << " ops (" << cumulative_lookups
            << " lookups, " << cumulative_inserts << " inserts, "
            <<  cumulative_scans << " scan)"
            << "\n------------------------------------------------------------"
            << "\n\tcumulative throughput:\t"
            << cumulative_inserts / cumulative_insert_time * 1e9
            << " inserts/sec,\t"
            << cumulative_lookups / cumulative_lookup_time * 1e9
            << " lookups/sec,\t"
            << cumulative_scans / cumulative_scan_time * 1e9
            << " scans/sec,\t"
            << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
            << "\n------------------------------------------------------------"
            << "\n\tcumulative elapsed time:\t"
            << "lookups: "
            << cumulative_lookup_time / 1e9
            << " sec,\t"
            << "inserts: "
            << cumulative_insert_time / 1e9
            << " sec,\t"
            << "scans: "
            << cumulative_scan_time / 1e9
            << " sec,\t"
            << "overall: "
            << cumulative_time / 1e9 << " sec"
            << std::endl;
  delete[] keys;
}
