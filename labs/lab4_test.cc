//
// Created by yhz on 2020-12-27.
//

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <experimental/filesystem>
#include <iomanip>
#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

#define assertm(exp, msg) assert(((void)msg, exp))

using namespace ROCKSDB_NAMESPACE;

inline char
random_char() {
  char charl[] = {
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N'};
  srand(static_cast<unsigned int >(time(nullptr)));
  int l = rand() % 3;
  switch (l) {
      case 0:
        return 'A' + rand() % 26;
      case 1:
        return 'a' + rand() % 26;
      case 2:
        return '0' + rand() % 10;
      default:
        return '-';
  }
}

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

void
test(DB* db, const std::vector<std::string>& keys, size_t total) {
    std::cout << "\n********** QPS **********\n" << std::endl;
    for (size_t i = 0; i < 5; i++) {
      std::cout << "\n====== Round " << i << " ======\n" << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      ReadOptions options1;
      std::string value;
      for (auto& jkey : keys) {
          db->Get(options1, jkey, &value);
      }
      auto end = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start);

      std::cout << "Total count: " << total << "\ttest count: " << keys.size()
                << "\ttotal time: " << duration.count() << " us"
                << "\tQPS: " << keys.size() * (1000 * 1000) / float(duration.count())
                << std::endl;
    }

  std::cout << "\n********** Latency **********\n" << std::endl;
  for (size_t m = 0; m < 5; m++) {
    std::vector<size_t> latencies;
    ReadOptions r_options;
    std::string v;
    for (auto& kkey : keys) {
      auto l_begin = std::chrono::high_resolution_clock::now();
      db->Get(r_options, kkey, &v);
      auto l_end = std::chrono::high_resolution_clock::now();
      auto l_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
          l_end - l_begin);
      latencies.emplace_back(l_duration.count());
    }

    auto max = std::max_element(std::begin(latencies), std::end(latencies));
    auto min = std::min_element(std::begin(latencies), std::end(latencies));
    size_t t_v = 0;
    for (auto& l : latencies) {
      t_v += l;
    }

    std::cout << "=== Round " << m << " ===\n\t"
              << "Max: " << *max << "ns" << "\t\tMin: " << *min << "ns"
              << "\t\tAvg: " << t_v / float(latencies.size()) << "ns" << std::endl;
  }
}

int main() {
  constexpr size_t total_count = 10 * 1000 * 1000;
  std::string ori_key = std::string(128, 'A');

  Options db_options;
  db_options.create_if_missing = true;
  WriteOptions w_options;

  /**
   * Short value
   */
  {
    std::string db_path = "/tmp/rocksdb/lab4_short_value";
    std::experimental::filesystem::remove_all(db_path);
    DB* db;
    Status s = DB::Open(db_options, db_path, &db);
    if (!s.ok()) {
      std::cerr << s.ToString() << std::endl;
      return 1;
    }
    //  std::array<std::string, total_count> keys;
    std::vector<std::string> keys;
    std::vector<std::string> seq_keys;
    std::string key = ori_key;
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
      if (i < 10000) {
        seq_keys.emplace_back(key);
      }

      key = key_inc(key);
    }

    db->Flush(FlushOptions());

    std::cout << "\n*****************************\n"
              << "********** short ************\n"
              << "*****************************\n"
              << std::endl;
    //
    std::cout << "\n>>>>> Random <<<<<\n" << std::endl;
    test(db, keys, total_count);
    std::cout << "\n>>>>> Seq <<<<<\n" << std::endl;
    test(db, seq_keys, total_count);
    //
  }
  /**
   * Long value
   */
  {
    std::string db_path = "/tmp/rocksdb/lab4_long_value";
    std::experimental::filesystem::remove_all(db_path);
    DB* db;
    Status s = DB::Open(db_options, db_path, &db);
    if (!s.ok()) {
      std::cerr << s.ToString() << std::endl;
      return 1;
    }
    std::vector<std::string> keys2;
    std::vector<std::string> seq_keys2;
    std::string key2 = ori_key;
    for (size_t j = 0; j < total_count; j++) {
      //    std::string v = std::to_string(i);
      auto key_s = Slice(key2);
      auto v = std::shared_ptr<char[]>(new char[256]());
      memset(v.get(), random_char(), 256);
      auto v_s = Slice(v.get(), 256);
      s = db->Put(w_options, key_s, v_s);
      if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return 1;
      }

      if (j % (total_count / 10) == 0) {
        std::cout << "< " << j << "/" << total_count << " > ... " << std::endl;
      }

      if (j % 1000 == 0) {
        keys2.emplace_back(key2);
      }

      if (j < 10000) {
        seq_keys2.emplace_back(key2);
      }

      key2 = key_inc(key2);
    }

    db->Flush(FlushOptions());
//    db->GetApproximateSizes()
    std::cout << "\n*****************************\n"
              << "*********** seq *************\n"
              << "*****************************\n"
              << std::endl;

    std::cout << "\n>>>>> Random <<<<<\n" << std::endl;
    test(db, keys2, total_count);
    std::cout << "\n>>>>> Seq <<<<<\n" << std::endl;
    test(db, seq_keys2, total_count);
  }

  return 0;
}
