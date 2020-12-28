//
// Created by yhz on 2020-12-27.
//

#include <algorithm>
#include <chrono>
#include <experimental/filesystem>
#include <iomanip>
#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#define assertm(exp, msg) assert(((void)msg, exp))

using namespace ROCKSDB_NAMESPACE;

inline void
add_key_item(std::string& s, size_t index) {
  if (s[index] == 'Z') {
    s[index] = 'A';

    if (index > 0)
      add_key_item(s, index - 1);
  } else {
    char c = s[index];
    s[index] = c + 1;
  }
}

std::string
key_inc(const std::string& key) {
  std::string new_key = key;
  size_t len = key.length();
  add_key_item(new_key, len -1);
  return new_key;
}

int main() {
  std::string db_path = "/tmp/rocksdb/query_performance";

  std::experimental::filesystem::remove_all(db_path);

  Options options;
  options.create_if_missing = true;
  DB* db;
  Status s = DB::Open(options, db_path, &db);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }

  WriteOptions w_options;

  constexpr size_t total_count = 10 * 1000 * 1000;

  std::string key = std::string(8, 'A');
//  std::array<std::string, total_count> keys;
  std::vector<std::string> keys;

  for (size_t i = 0; i < total_count; i++) {

      std::string v = std::to_string(i);
      auto key_s = Slice(key);
      auto v_s = Slice(v);
      s = db->Put(w_options, key_s, v_s);
      if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return 1;
      }

    if (i % (total_count / 10) == 0) {
      std::cout << "< " << i << "/" << total_count << " > ... " << std::endl;
    }

    if (i % 1000 == 0) {
      keys.emplace_back(key);
    }

      key = key_inc(key);
  }

  db->Flush(FlushOptions());

  std::cout << "\n============== Read =============\n" << std::endl;

  for (size_t j = 0; j < 5; j++) {
    std::cout << "\n====== Round " << j <<" ======\n" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    ReadOptions options1;
    std::string value;
    for (auto& jkey : keys) {
      db->Get(options1, jkey, &value);
      //    if (j % 100000 == 0) {
      //      std::cout << "< " << j << "/" << total_count << " > ...  value " << value << std::endl;
      //    }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << std::setw(10) << "result" << std::setw(20) << "total count" << std::setw(20) << "total time"
              << std::setw(40) << "value"
              << "\n"
              << std::setw(10) << "QPS" << std::setw(20) << keys.size()
              << std::setw(20) << std::to_string(duration.count() / 1000.0f) + " ms"
              << std::setw(40)
              << std::to_string(keys.size() / float(duration.count()) * 1000 * 1000) +
                     " entities / s"
              << "\n"
              << std::setw(10) << "Latency" << std::setw(20) << keys.size()
              << std::setw(20) << std::to_string(duration.count() / 1000.0f) + " ms"
              << std::setw(40)
              << std::to_string(duration.count() / (float(keys.size()))) +
                     " us / entities"
              << std::endl;
  }

  for (size_t m = 0; m < 5; m++) {
    std::vector<size_t> latencies;
    ReadOptions r_options;
    std::string v;
    for (auto& kkey : keys) {
      auto l_begin = std::chrono::high_resolution_clock::now();
      db->Get(r_options, kkey, &v);
      auto l_end = std::chrono::high_resolution_clock::now();
      auto l_duration = std::chrono::duration_cast<std::chrono::microseconds>(
          l_end - l_begin);
      latencies.emplace_back(l_duration.count());
    }

    auto max = std::max_element(std::begin(latencies), std::end(latencies));
    auto min = std::min_element(std::begin(latencies), std::end(latencies));
    size_t t_v = 0;
    for (auto& l : latencies) {
      t_v += l;
    }

    std::cout << "\n\nLatency:\n"
              << "=== Round " << m << " ===\n\t"
              << "Max: " << *max << ", Min: " << *min
              << ", Avg: " << t_v / float(latencies.size()) << std::endl;
  }

  return 0;
}
