//
// Created by yhz on 2020-12-28.
//

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <experimental/filesystem>
#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

inline char random_char() {
  srand(static_cast<unsigned int>(time(nullptr)));
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

inline void add_key_item(std::string& s, size_t index) {
  if (s[index] == 'Z') {
    s[index] = 'A';

    if (index > 0) add_key_item(s, index - 1);
  } else {
    char c = s[index];
    s[index] = c + 1;
  }
}

std::string key_inc(const std::string& key) {
  std::string new_key = key;
  size_t len = key.length();
  add_key_item(new_key, len - 1);
  return new_key;
}

using Values = std::vector<std::shared_ptr<char[]>>;
using Keys = std::vector<std::string>;

void gen_keys(size_t count, const std::string& ori_key, Keys& keys) {
  keys.resize(count);
  std::string key = ori_key;
  for (size_t i = 0; i < count; i++) {
    keys[i] = key;
    key = key_inc(key);
  }
}

void gen_value(size_t count, size_t value_size, Values& vs) {
  vs.resize(count);
  for (size_t e = 0; e < count; e++) {
    auto v = std::shared_ptr<char[]>(new char[value_size]());
    //    for (size_t i = 0; i < value_size; i++) {
    //      v[i] = random_char();
    //    }
    memset(v.get(), random_char(), value_size);
    vs[e] = v;
//    if (e % 100000 == 0) {
//      std::cout << "< " << e << "/" << count << " > Gen value ... ";
//    }
  }
}

void test(size_t key_size, size_t value_size) {
  size_t entity_size = key_size + value_size;
  size_t total_to_inserted_count = 1 * 1024 * 1024 * 1024 / entity_size;
  size_t stage_count = total_to_inserted_count / 4;

  std::string ori_key = std::string(key_size, 'A');
  std::vector<std::string> keys(stage_count * 4);
  gen_keys(stage_count * 4, ori_key, keys);

  Values values(stage_count * 4);
  gen_value(stage_count * 4, value_size, values);

  Options db_options;
  db_options.create_if_missing = true;
  WriteOptions w_options;

  std::string db_path = "/tmp/rocksdb/la3_key_" + std::to_string(key_size) +
                        "_value_" + std::to_string(value_size);
  std::experimental::filesystem::remove_all(db_path);
//  std::string key = ori_key;

  DB* db;
  Status s = DB::Open(db_options, db_path, &db);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    throw std::runtime_error(s.ToString());
  }

  for (size_t r = 0; r < 4; r++) {
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < stage_count; i++) {
      size_t offset = r * stage_count + i;
      auto v_s = Slice(values[offset].get(), value_size);
      db->Put(w_options, keys[offset], v_s);
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration<double, std::nano>(end - start);

    std::cout << "===== stage " << r << " =====\n"
              << "Inserted count: " << stage_count
              << "\t total inserted count: " << stage_count * (r + 1)
              << "\ttotal time: " << duration.count() * 0.001 * 0.001 << " ms"
              << "\tQPS: "
              << stage_count * 1000 * 1000 * 1000 / duration.count()
              << "\tLatency: " << duration.count() / stage_count << " ns"
              << std::endl;

    /** read **/
    size_t current_inserted_count = (r + 1) * stage_count;
    size_t query_count = 50000 < stage_count * 4 ? 50000 : stage_count * 4;
    size_t query_stage = current_inserted_count / query_count;

    ReadOptions r_options;
    std::string value;

    for (size_t q = 0; q < 5; q++) {
      auto query_start = std::chrono::high_resolution_clock::now();
      for (size_t m = 0; m < query_count; m++) {
        db->Get(r_options, keys[m * query_stage], &value);
      }
      auto query_end = std::chrono::high_resolution_clock::now();
      auto query_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(query_end -
                                                                query_start);

      std::cout << "[Query round " << q << "]\t\t"
                << "Total count: " << current_inserted_count
                << "\ttest count: " << query_count
                << "\ttotal time: " << query_duration.count() << " us"
                << "\tQPS: "
                << (query_count * 1000 * 1000.0f) /
                       float(query_duration.count())
                << "\tLatency: "
                << float(query_duration.count()) / (query_count) << " us"
                << std::endl;
    }
  }
}

int main() {
  for (size_t value_size = 128; value_size <= /**128**/16 * 1024 * 1024; value_size *= 2) {
    std::cout << "\n\n[[ Value size " << value_size << " ]]\n" << std::endl;
    test(128, value_size);
  }

  return 0;
}
