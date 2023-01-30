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
 * --init_num_keys          number of keys to bulk load with
 * --total_num_keys         total number of keys in the keys file
 * --batch_size             number of operations (lookup or insert) per batch
 *
 * Optional flags:
 * --lookup_distribution    lookup keys distribution (options: uniform or zipf)
 * --time_limit             time limit, in minutes
 * --print_batch_stats      whether to output stats for each batch
 */
int main(int argc, char* argv[]) {
  auto flags = parse_flags(argc, argv);
  std::string keys_file_path = get_required(flags, "keys_file");
  std::string keys_file_type = get_required(flags, "keys_file_type");
  auto init_num_keys = stoi(get_required(flags, "init_num_keys"));
  auto total_num_keys = stoi(get_required(flags, "total_num_keys"));
  auto batch_size = stoi(get_required(flags, "batch_size"));
  std::string lookup_distribution =
      get_with_default(flags, "lookup_distribution", "zipf");
  auto time_limit = stod(get_with_default(flags, "time_limit", "1.0"));
  bool print_batch_stats = get_boolean_flag(flags, "print_batch_stats");
  auto range_size = stoi(get_required(flags, "range_size"));
  bool read_modify_write = false; // true iff workload F
  const size_t kInitialTableSize = 16*1024;

  std::string workload = get_required(flags, "workload");

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
  std::cout << "DTS version" << std::endl;
  DyTIS* index = new DyTIS();
  for (int load_num = 0; load_num < init_num_keys; load_num++) {
    index->Insert(keys[load_num], static_cast<PAYLOAD_TYPE>(gen_payload()));
  }


  // Run workload
  int i = init_num_keys;
  long long cumulative_inserts = 0;
  long long cumulative_updates = 0;
  long long cumulative_lookups = 0;
  long long cumulative_scans = 0;
  long long cumulative_operations = 0;

  int num_inserts_per_batch = 0;
  int num_updates_per_batch = 0;
  int num_lookups_per_batch = 0;
  int num_scans_per_batch = 0;

  double insert_frac = 0.0; // used for workload Load, D and E
  double update_frac = 0.0; // it would be update for A and B \
                                or read-modify-write for F
  double lookup_frac = 0.0; // used for workload A, B, C, D and F
  double scan_frac = 0.0; // used for workload E

  if (workload == "Load") {
    insert_frac = 1.0;
    num_inserts_per_batch = static_cast<int> \
                            ((total_num_keys - init_num_keys) * insert_frac);
    std::cout << "WORKLOAD Load" << std::endl;
  }
  else if (workload == "A") {
    update_frac = 0.5;
    lookup_frac = 1.0 - (insert_frac + update_frac + lookup_frac + scan_frac);

    num_updates_per_batch = static_cast<int>(batch_size * update_frac);
    num_lookups_per_batch = batch_size - num_updates_per_batch;

    std::cout << "WORKLOAD A" << std::endl;
  }
  else if (workload == "B") {
    update_frac = 0.05;
    lookup_frac = 1.0 - (insert_frac + update_frac + lookup_frac + scan_frac);

    num_updates_per_batch = static_cast<int>(batch_size * update_frac);
    num_lookups_per_batch = batch_size - num_updates_per_batch;

    std::cout << "WORKLOAD B" << std::endl;
  }
  else if (workload == "C") {
    lookup_frac = 1.0 - (insert_frac + update_frac + lookup_frac + scan_frac);

    num_lookups_per_batch = batch_size - num_updates_per_batch;

    std::cout << "WORKLOAD C" << std::endl;
  }
  else if (workload == "D") {
    insert_frac = 0.05;
    lookup_frac = 1.0 - (insert_frac + update_frac + lookup_frac + scan_frac);

    num_inserts_per_batch = static_cast<int>(batch_size * insert_frac);
    num_lookups_per_batch = batch_size - num_inserts_per_batch;

    std::cout << "WORKLOAD D" << std::endl;
  }
  else if (workload == "E") {
    insert_frac = 0.05;
    scan_frac = 1.0 - (insert_frac + update_frac + lookup_frac + scan_frac);

    num_scans_per_batch = static_cast<int>(batch_size * scan_frac);
    num_inserts_per_batch = batch_size - num_scans_per_batch;

    std::cout << "WORKLOAD E" << std::endl;
  }
  else if (workload == "F") {
    read_modify_write = true;
    update_frac = 0.5;
    lookup_frac = 1.0 - (insert_frac + lookup_frac + scan_frac);

    num_updates_per_batch = static_cast<int>(batch_size * update_frac);
    num_lookups_per_batch = batch_size - num_updates_per_batch;

    std::cout << "WORKLOAD F" << std::endl;
  }
  else {
    std::cout << "Not support such workload: " << workload << std::endl;
    return 1;
  }


  double cumulative_insert_time = 0;
  double cumulative_update_time = 0;
  double cumulative_lookup_time = 0;
  double cumulative_scan_time = 0;
  std::cout << "num_inserts_per_batch: " << num_inserts_per_batch << std::endl;
  std::cout << "num_updates_per_batch: " << num_updates_per_batch << std::endl;
  std::cout << "num_lookups_per_batch: " << num_lookups_per_batch << std::endl;
  std::cout << "num_scans_per_batch: " << num_scans_per_batch << std::endl;


  int batch_no = 0;
  int log_count = 1;
  auto workload_start_time = std::chrono::high_resolution_clock::now();

  while (1) {
    batch_no++;
    // Do lookups
    if (lookup_frac) {
      double batch_lookup_time = 0.0;
      if (i > 0) {
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
      auto lookups_start_time = std::chrono::high_resolution_clock::now();
      for (int j = 0; j < num_lookups_per_batch; j++) {
        index->Get(lookup_keys[j]);
      }
      auto lookups_end_time = std::chrono::high_resolution_clock::now();
      batch_lookup_time =
          std::chrono::duration_cast<std::chrono::nanoseconds>(lookups_end_time -
                                                               lookups_start_time)
              .count();

      cumulative_lookup_time += batch_lookup_time;
      cumulative_lookups += num_lookups_per_batch;
      cumulative_operations += num_lookups_per_batch;
      delete[] lookup_keys;
      }
    }

    // Do scans
    if (scan_frac) {
      KEY_TYPE* scan_start_keys = nullptr;
      if (lookup_distribution == "uniform") {
        scan_start_keys = get_search_keys(keys, i, num_scans_per_batch);
      }
      else {
        scan_start_keys = get_search_keys_zipf(keys, i, num_scans_per_batch);
      }

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
      cumulative_operations += num_scans_per_batch;
      delete[] scan_start_keys;
    }

    // Do updates
    if (update_frac) {

      int num_actual_updates = num_updates_per_batch;

      KEY_TYPE* update_keys = nullptr;
      // update will follow lookup distribution
      if (lookup_distribution == "uniform") {
        update_keys = get_search_keys(keys, i, num_updates_per_batch);
      } else if (lookup_distribution == "zipf") {
        update_keys = get_search_keys_zipf(keys, i, num_updates_per_batch);
      } else {
        std::cerr << "--lookup_distribution must be either 'uniform' or 'zipf'"
                  << std::endl;
        return 1;
      }
      auto updates_start_time = std::chrono::high_resolution_clock::now();
      for (int j = 0; j < num_updates_per_batch; j++) {
        if (!read_modify_write) {// update
          index->Update(update_keys[j], static_cast<PAYLOAD_TYPE>(gen_payload()));
        }
        else { // if Workload F, try to search the key first and do update
          index->Get(update_keys[j]);
          index->Update(update_keys[j], static_cast<PAYLOAD_TYPE>(gen_payload()));
        }
      }
      auto updates_end_time = std::chrono::high_resolution_clock::now();
      delete[] update_keys;
      double batch_update_time =
          std::chrono::duration_cast<std::chrono::nanoseconds>(updates_end_time -
                                                               updates_start_time)
              .count();
      cumulative_update_time += batch_update_time;
      cumulative_updates += num_actual_updates;
      cumulative_operations += num_actual_updates;
    }

    // Do inserts
    if (insert_frac) {
      int num_actual_inserts =
          std::min(num_inserts_per_batch, total_num_keys - i);
      int num_keys_after_batch = i + num_actual_inserts;

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
      cumulative_operations += num_actual_inserts;

      // For workload D and E, run the experiment
      // until all the keys in the dataset are inserted.
      if (i >= total_num_keys) {
        std::cout << "insert over!!" << std::endl;
        break;
      }
    }
    else {
      double workload_elapsed_time =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::high_resolution_clock::now() - workload_start_time)
              .count();
      // For others, run the experiment until the number of operations performed
      // is more than 50% of the dataset size for at least 60 second.
      if (workload_elapsed_time > time_limit * 1e9 *60) {
        // run at least half of the dataset
        if (batch_no >= 5)
          break;
        else
          std::cout << "cumulative_operations is only " << \
          (int)((double)cumulative_operations*100/total_num_keys + 0.5) << \
          "%, batch_no : " << batch_no << std::endl;
      }
      else {
        if (workload_elapsed_time > time_limit * 1e9 * 10 * log_count) {
          std::cout << batch_no << " batches done" << std::endl;
          log_count++;
        }
      }
    }
  }


  double cumulative_time = cumulative_lookup_time + cumulative_update_time \
                           + cumulative_insert_time + cumulative_scan_time;
  std::cout << std::scientific;
  std::cout << std::setprecision(2);
  std::cout << "Cumulative stats: " << batch_no << " batches, "
            << cumulative_operations << " ops (" << cumulative_updates
            << " updates, " << cumulative_inserts << " inserts, "
            << cumulative_lookups << " lookups, "
            <<  cumulative_scans << " scan)"
            << "\n------------------------------------------------------------"
            << "\n\tcumulative throughput:\t"
            << (cumulative_update_time == 0 ? 0 : \
                cumulative_updates / cumulative_update_time * 1e9)
            << " updates/sec,\t"
            << (cumulative_insert_time == 0 ? 0 : \
                cumulative_inserts / cumulative_insert_time * 1e9)
            << " inserts/sec,\t"
            << (cumulative_lookup_time == 0 ? 0 : \
                cumulative_lookups / cumulative_lookup_time * 1e9)
            << " lookups/sec,\t"
            << (cumulative_scan_time == 0 ? 0 : \
                cumulative_scans / cumulative_scan_time * 1e9)
            << " scans/sec,\t"
            << cumulative_operations / cumulative_time * 1e9 << " ops/sec"
            << "\n------------------------------------------------------------"
            << "\n\tcumulative elapsed time:\t"
            << "updates: "
            << cumulative_update_time / 1e9
            << " sec,\t"
            << "inserts: "
            << cumulative_insert_time / 1e9
            << " sec,\t"
            << "lookups: "
            << cumulative_lookup_time / 1e9
            << " sec,\t"
            << "scans: "
            << cumulative_scan_time / 1e9
            << " sec,\t"
            << "overall: "
            << cumulative_time / 1e9 << " sec"
            << std::endl;

  delete[] keys;

}
