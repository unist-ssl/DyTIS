// Code modified from https://github.com/microsoft/ALEX/tree/57efb5005bd0e769ecee9c2bc75125f8ea340730/src/benchmark
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "zipf.h"
#include <fstream>
#include <utility>
#include <cstring>

template <class T>
bool load_binary_data(T data[], int length, const std::string& file_path) {
  std::ifstream is(file_path.c_str(), std::ios::binary | std::ios::in);
  if (!is.is_open()) {
    return false;
  }
  is.read(reinterpret_cast<char*>(data), std::streamsize(length * sizeof(T)));
  is.close();
  return true;
}

template <class T>
bool load_text_data(T array[], int length, const std::string& file_path) {
  std::ifstream is(file_path.c_str());
  if (!is.is_open()) {
    return false;
  }
  int i = 0;
  std::string str;
  while (std::getline(is, str) && i < length) {
    std::istringstream ss(str);
    ss >> array[i];
    i++;
  }
  is.close();
  return true;
}

std::vector<std::pair<char, uint64_t> >load_run_text_data(int length, const std::string& file_path) {
  std::ifstream is(file_path.c_str());
  if (!is.is_open()) {
    std::cout << "file not exist" << std::endl;
    exit(1);
  }
  std::vector<std::pair<char, uint64_t> > array;
  int i = 0;
  std::string str;
  char* buf = new char[100];
  while (std::getline(is, str) && i < length) {
    /*
    std::istringstream ss(str);
    ss >> array[i];
    i++;
    */
    strcpy(buf, str.c_str());
    char operation_type;
    uint64_t key;
    sscanf(buf, "%c %lu\n", &operation_type, &key);
    std::pair inserted = std::make_pair(operation_type, key);
    array.push_back(inserted);
  }
  is.close();
  delete[] buf;
  return array;
}
// Lehmer random number generator
// https://en.wikipedia.org/wiki/Lehmer_random_number_generator
// http://thompsonsed.co.uk/random-number-generators-for-c-performance-tested
uint64_t next(uint64_t x) {
  const uint64_t modulus = static_cast<unsigned long>(pow(2, 32));
  const uint64_t multiplier = 1664525;
  const uint64_t increment = 1013904223;
  x = (multiplier * x + increment) % modulus;
  return x;
}

template <class T>
T* get_search_keys(T array[], int num_keys, int num_searches) {
  std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<int> dis(0, num_keys - 1);
  auto* keys = new T[num_searches];
  for (int i = 0; i < num_searches; i++) {
    int pos = dis(gen);
    keys[i] = array[pos];
  }
  return keys;
}

template <class T>
T* get_search_keys_zipf(T array[], int num_keys, int num_searches) {
  auto* keys = new T[num_searches];
  ScrambledZipfianGenerator zipf_gen(num_keys);
  for (int i = 0; i < num_searches; i++) {
    int pos = zipf_gen.nextValue();
    keys[i] = array[pos];
  }
  return keys;
}
