// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#pragma once

#include <thread>
#include <string>
#include "random"

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

inline int64_t kget_time_now() {
  struct timespec timestamp;
  clock_gettime(CLOCK_REALTIME, &timestamp);
  return timestamp.tv_sec * 1000000000 + timestamp.tv_nsec;
}

#define KWDB_TIME (kget_time_now())
// Control whether performance statistics are enabled by compiling macros
#ifdef KWDB_STATS_ON
#define KWDB_START_T() auto __kwdb_start__ = KWDB_TIME;
#define KWDB_END_T(x) x.add(KWDB_TIME - __kwdb_start__);
#define KWDB_DURATION(x) DeferStat __kwdb_stat__(x);
#define KWDB_STAT_ADD(x, num) x.add(num);
#else
#define KWDB_START_T()
#define KWDB_END_T(x)
#define KWDB_DURATION(x)
#define KWDB_STAT_ADD(x, num)
#endif

namespace kwdbts {
namespace random {
// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package.
class Random {
 private:
  enum : uint32_t {
    M = 2147483647L  // 2^31-1
  };
  enum : uint64_t {
    A = 16807  // bits 14, 8, 7, 5, 2, 1, 0
  };

  uint32_t seed_;

  static uint32_t GoodSeed(uint32_t s) { return (s & M) != 0 ? (s & M) : 1; }

 public:
  // This is the largest value that can be returned from Next()
  enum : uint32_t {
    kMaxNext = M
  };

  explicit Random(uint32_t s) : seed_(GoodSeed(s)) {}

  void Reset(uint32_t s) { seed_ = GoodSeed(s); }

  uint32_t Next() {
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }

  // Returns a Random instance for use by the current thread without
  // additional locking
  static Random* GetTLSInstance() {
    static __thread Random* tls_instance;
    static __thread std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;
    auto rv = tls_instance;
    if (UNLIKELY(rv == nullptr)) {
      size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
      rv = new(&tls_instance_bytes) Random((uint32_t) seed);
      tls_instance = rv;
    }
    return rv;
  }
};

}/* namespace random */

struct AvgStat {
 private:
  const int NUM = 100;
  uint64_t* sum_ = nullptr;
  uint64_t* size_ = nullptr;
  const double scale = 1.0;
  const std::string str;

 public:
  AvgStat() {
    sum_ = new uint64_t[NUM];
    size_ = new uint64_t[NUM];
    for (int i = 0 ; i < NUM ; i++) {
      sum_[i] = 0;
      size_[i] = 0;
    }
  }

  explicit AvgStat(const std::string& s) : str(s) {
    sum_ = new uint64_t[NUM];
    size_ = new uint64_t[NUM];
    for (int i = 0 ; i < NUM ; i++) {
      sum_[i] = 0;
      size_[i] = 0;
    }
  }

  ~AvgStat() {
    if (sum_ != nullptr)
      delete[] sum_;
    if (size_ != nullptr)
      delete[] size_;
  }

  void add(double value) {
    // int i = rand() % NUM;
    int i = random::Random::GetTLSInstance()->Next() % NUM;
    __sync_fetch_and_add(&(sum_[i]), (uint64_t) (value * scale));
    __sync_fetch_and_add(&(size_[i]), 1);
  }

  double avg() {
    double total_sum = 0.0;
    uint64_t total_size = 0;
    for (int i = 0 ; i < NUM ; i++) {
      total_sum += sum_[i] * 1.0 / scale;
      total_size += size_[i];
    }
    if (total_size == 0) return 0;
    return total_sum / total_size;
  }

  double sum() {
    double total_sum = 0;
    for (int i = 0 ; i < NUM ; i++) {
      total_sum += sum_[i] * 1.0 / scale;
    }
    return total_sum;
  }

  double max() {
    double max_value = 0;  // Compare only positive numbers
    for (int i = 0 ; i < NUM ; i++) {
      if (max_value < sum_[i]) {
        max_value = sum_[i];
      }
    }
    return max_value;
  }

  uint64_t size() {
    uint64_t total_size = 0;
    for (int i = 0 ; i < NUM ; i++) {
      total_size += size_[i];
    }
    return total_size;
  }

  void reset() {
    if (size_ == nullptr || sum_ == nullptr)
      return;
    for (int i = 0 ; i < NUM ; i++) {
      sum_[i] = 0;
      size_[i] = 0;
    }
  }

  void print(std::string s) {
    printf("%s: %lf / %ld = %lf\n",
           s.c_str(), sum(), size(), avg());
  }

  void print() {
    printf("%s: %lf / %ld = %lf\n",
           str.c_str(), sum(), size(), avg());
  }
};

class DeferStat {
 public:
  inline explicit DeferStat(AvgStat& stat) : stat_(stat) { d_start_ = KWDB_TIME; }

  inline ~DeferStat() { stat_.add(KWDB_TIME - d_start_); }

 private:
  uint64_t d_start_;
  AvgStat& stat_;
};

// Statistical performance loss of read and write paths
struct StStatistics {
  // Metadata section
  AvgStat create_table;  // TSCreateTsTable
  AvgStat create_group;  // TSCreateRangeGroup
  // Data writing and parsing section
  AvgStat ts_put;  // TSPutData
  AvgStat alloc_entity;  // TsEntityGroup::allocateEntityGroupId
  AvgStat get_partition;  // SubEntityGroupManager::GetPartitionTable
  AvgStat put_tag;  // TsEntityGroup::putTagData
  AvgStat put_data;  // TsEntityGroup::putDataColumnar
  AvgStat payload_row;  // batch row
  AvgStat push_payload;  //  MMapPartitionTable::push_back_payload
  AvgStat alloc_space;  //  MMapPartitionTable::AllocateSpace
  AvgStat dedup_before;  //  MMapPartitionTable::DedupByPayloadBefore
  AvgStat dedup_after;  //  MMapPartitionTable::DedupByPayloadAfter
  // AvgStat get_partition;  // MMapSegmentTable::PushPayload
  AvgStat segment_pushdata;  // MMapSegmentTable::PushPayload data
  AvgStat segment_pushagg;  // MMapSegmentTable::PushPayload agg

  // Iterator query section
  AvgStat dml_setup;  //  DmlExec::ExecQuery
  AvgStat dml_next;  // DmlExec::ExecQuery
  AvgStat get_partitions;  // SubEntityGroupManager::GetPartitionTables
  AvgStat partition_num;  // TsEntityGroup::GetIterator, The number of partitions obtained each time
  AvgStat get_iterator;  // TsTable::GetIterator
  AvgStat it_next;  // TsRowDataIterator::Next
  AvgStat it_num;  // get row num
  AvgStat agg_next;  // TsAggIterator::Next
  AvgStat agg_max;  // Calculation time for each aggregated column
  AvgStat agg_min;
  AvgStat agg_sum;
  AvgStat agg_count;
  AvgStat agg_first;
  AvgStat agg_last;
  AvgStat agg_lastrow;
  AvgStat agg_firstts;
  AvgStat agg_lastts;
  AvgStat agg_blocks;  // TsAggIterator::TraverseAllBlocks

  static StStatistics& Get() {
    static StStatistics w_stats_;
    return w_stats_;
  }

  ~StStatistics() {
    Show(true);
  }

  void Show(bool exit = false) {
#ifndef KWDB_STATS_ON
    return;
#endif
    char buf[2048];
    snprintf(buf, sizeof(buf), "\n*******Write Statistics (ns) ******\n"
                               " Engine ::: createTable=%ld(%ld),createRange=%ld(%ld)\n"
                               " TsTable ::: TsPut=%ld(%ld),alloc_entity=%ld(%ld),get_partition=%ld(%ld), "
                               "put_tag=%ld(%ld), put_data=%ld(%ld,%.1f) \n"
                               " PartitionTable ::: push_payload=%ld,alloc_space=%ld,dedup_before=%ld,"
                               " dedup_after=%ld,segment_pushdata=%ld, segment_pushagg=%ld \n"
                               "**********************************\n",
             (int64_t) create_table.avg(), create_table.size(), (int64_t) create_group.avg(), create_group.size(),
             (int64_t) ts_put.avg(), ts_put.size(),
             (int64_t) alloc_entity.avg(), alloc_entity.size(), (int64_t) get_partition.avg(), get_partition.size(),
             (int64_t) put_tag.avg(), put_tag.size(), (int64_t) put_data.avg(), put_data.size(), payload_row.avg(),
             (int64_t) push_payload.avg(), (int64_t) alloc_space.avg(), (int64_t) dedup_before.avg(),
             (int64_t) dedup_after.avg(), (int64_t) segment_pushdata.avg(), (int64_t) segment_pushagg.avg());
    LOG_INFO("%s", buf);
    ResetWrite();
  }

  void ShowQuery(bool exit = false) {
#ifndef KWDB_STATS_ON
    return;
#endif
    char buf[2048];
    snprintf(buf, sizeof(buf), "\n*******Query Statistics (ns) ******\n"
                               " Execute ::: dml_setup=%.2f(%ld) ms,dml_next=%.2f(%ld) ms,"
                               "get_iterator=%ld(%ld),get_partitions=%ld(%ld,%.1f),"
                               "it_next=%ld(%ld),it_num=%ld,agg_next=%ld(%ld)\n"
                               " Aggregate ::: max=%ld(%ld),min=%ld(%ld),"
                               "sum=%ld(%ld),count=%ld(%ld),"
                               "first=%ld(%ld),last=%ld(%ld),firstts=%ld(%ld),lastts=%ld(%ld),"
                               "last_row=%ld(%ld),blocks=%ld(%ld)\n"
                               "**********************************\n",
             dml_setup.avg() / 1e6, dml_setup.size(), dml_next.avg() / 1e6, dml_next.size(),
             (int64_t) get_iterator.avg(), get_iterator.size(), (int64_t) get_partitions.avg(), get_partitions.size(),
             partition_num.avg(),
             (int64_t) it_next.avg(), it_next.size(), (int64_t) it_num.avg(), (int64_t) agg_next.avg(), agg_next.size(),
             (int64_t) agg_max.avg(), agg_max.size(), (int64_t) agg_min.avg(), agg_min.size(),
             (int64_t) agg_sum.avg(), agg_sum.size(), (int64_t) agg_count.avg(), agg_count.size(),
             (int64_t) agg_first.avg(), agg_first.size(), (int64_t) agg_last.avg(), agg_last.size(),
             (int64_t) agg_firstts.avg(), agg_firstts.size(), (int64_t) agg_lastts.avg(), agg_lastts.size(),
             (int64_t) agg_lastrow.avg(), agg_lastrow.size(), (int64_t) agg_blocks.avg(), agg_blocks.size());
    LOG_INFO("%s", buf);
    ResetQuery();
  }

  void ResetWrite() {
    create_table.reset();
    create_group.reset();
    ts_put.reset();
    alloc_entity.reset();
    get_partition.reset();
    put_tag.reset();
    put_data.reset();
    payload_row.reset();
    push_payload.reset();
    alloc_space.reset();
    dedup_before.reset();
    dedup_after.reset();
    //  get_partition.reset();
    segment_pushdata.reset();
    segment_pushagg.reset();
  }
  void ResetQuery() {
    dml_setup.reset();
    dml_next.reset();
    get_iterator.reset();
    get_partitions.reset();
    partition_num.reset();
    it_next.reset();
    it_num.reset();
    agg_next.reset();
    agg_max.reset();
    agg_min.reset();
    agg_sum.reset();
    agg_count.reset();
    agg_first.reset();
    agg_last.reset();
    agg_firstts.reset();
    agg_lastts.reset();
    agg_lastrow.reset();
    agg_blocks.reset();
  }

 private:
  static const int64_t KB = 1024;
  static const int64_t MS = 1e6;
  static const int64_t Second = 1e9;
};

}  //  namespace kwdbts
