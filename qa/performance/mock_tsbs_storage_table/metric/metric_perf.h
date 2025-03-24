#pragma once
#include <gtest/gtest.h>
#include <random>
#include <chrono>
#include <fstream>
#include <filesystem>
#include <utility>
#include <memory>
#include "utils/big_table_utils.h"
#include "ts_table.h"
#include "payload_generator.h"
#include "common.h"
#include "tag_perf.h"

using namespace std;
using namespace kwdbts;

#define DEF_ATTR(attr, T, S, L) \
  AttributeInfo attr; attr.name = (#attr); attr.type = (T); attr.size = (S); attr.length = (L)

class MetricTablePerformance final {
 public:
  explicit MetricTablePerformance(const vector<AttributeInfo>& schema, const vector<TagInfo>& tag, const string& home, const string& dbname)
    : schema_(schema),
      tag_(tag),
      home_(home),
      dbname_(dbname) {
    //
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
  }

  ~MetricTablePerformance() {
    // remove();
  }

  int create(uint64_t table_id) {
    table_ = new TsTableImplctx_, filesystem::path(home_) / filesystem::path(dbname_), table_id);
    table_->Init(ctx_);
    table_->DropAll(ctx_);

    auto s = table_->Create(ctx_, schema_);
    if (s != KStatus::SUCCESS) {
      cerr << "Create failed: " << s << endl;
      return -1;
    }
    s = table_->CreateEntityGroup(ctx_, {group_id_, 0}, tag_, &entity_group_);
    if (s != KStatus::SUCCESS) {
      cerr << "CreateEntityGroup failed: " << s << endl;
      return -1;
    }
    s = table_->GetEntityGroup(ctx_, group_id_, &entity_group_);
    if (s != KStatus::SUCCESS) {
      cerr << "GetEntityGroup failed: " << s << endl;
      return -1;
    }
    return 0;
  }

  int open(uint64_t table_id) {
    table_ = new TsTableImpl(ctx_, filesystem::path(home_) / filesystem::path(dbname_), table_id);
    auto s = table_->Init(ctx_);
    if (s != KStatus::SUCCESS) {
      cerr << "Init failed" << endl;
      return -1;
    }
    s = table_->UpdateEntityGroup(ctx_, {group_id_, 0});
    if (s != KStatus::SUCCESS) {
      cerr << "UpdateEntityGroup failed" << endl;
      return -1;
    }
    return 0;
  }

  int remove() {
    if (table_) {
      table_->DropAll(ctx_);
    }
    return 0;
  }

  int insert(kwdbContext_p ctx, void * payload, uint64_t len) {
    if (!entity_group_) {
      cerr << "Invalid entity group" << endl;
      return -1;
    }
    auto s = entity_group_->PutData(ctx, TSSlice{static_cast<char *>(payload), len});
    if (s != KStatus::SUCCESS) {
      cerr << "PutData failed: " << s << endl;
      return -1;
    }
    return 0;
  }

  size_t traverse_tag_by_iterator(kwdbContext_p ctx) {
    std::vector<EntityResultIndex> entityIdList;
    std::vector<uint32_t> scan_tags;
    for (auto i = 0; i < tag_.size(); ++i) {
      scan_tags.emplace_back(i);
    }
    TagIterator *iter;
    auto s = table_->GetTagIterator(ctx, scan_tags, &iter);
    if (s != KStatus::SUCCESS) {
      cerr << "GetTagIterator failed" << endl;
      return -1;
    }

    ResultSet res;
    k_uint32 count = 0;
    size_t total = 0;
    do {
      s = iter->Next(&entityIdList, &res, &count);
      if (s != KStatus::SUCCESS) {
        cerr << "Next failed" << endl;
        iter->Close();
        delete iter;
        return -1;
      }
      total += count;
      entityIdList.clear();
      res.clear();
    } while(count);

    // destroy
    iter->Close();
    delete iter;
    return total;
  }

 private:
  vector<AttributeInfo> schema_;
  vector<TagInfo> tag_;
  std::string home_;
  std::string dbname_;
  TsTable * table_{};
  kwdbContext_t context_{};
  kwdbContext_p ctx_{};
  uint64_t group_id_ = 100;
  std::shared_ptr<TsEntityGroup> entity_group_;
};

struct MetricTestParam {
  size_t thread_count;
  size_t device_count;
  size_t row_count_per_dev;
};

class MetricInsertTest : public TestCommon, public ::testing::TestWithParam<MetricTestParam> {
 public:
  MetricInsertTest() {
    db_name_ = "perf_metric";
    iot_interval_ = 3600;

    DEF_ATTR(k_timestamp, DATATYPE::TIMESTAMP64, 8, 8);
    DEF_ATTR(usage_user, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_system, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_idle, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_nice, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_iowait, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_irq, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_softirq, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_steal, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_guest, DATATYPE::INT64, 8, 8);
    DEF_ATTR(usage_guest_nice, DATATYPE::INT64, 8, 8);
    metric_schema_ = {std::move(k_timestamp), std::move(usage_user), std::move(usage_system), std::move(usage_idle),
                      std::move(usage_nice), std::move(usage_iowait), std::move(usage_irq), std::move(usage_softirq),
                      std::move(usage_steal), std::move(usage_guest), std::move(usage_guest_nice)};
    pg_.reset(new PayloadGenerator(metric_schema_));
    tpg_.reset(new TagPayloadGenerator(tag_schema_));
    pg_->set_tb(10086);
  }

  ~MetricInsertTest() override = default;

  void SetUp() override {
    create_db();
  }

  void TearDown() override {}

 public:
  void perf_insert(size_t dev_cnt, size_t thr_cnt, size_t row_cnt_per_dev) {
    if (dev_cnt != 100 && dev_cnt != 4000 && dev_cnt != 100000 && dev_cnt != 1000000 && dev_cnt != 10000000) {
      cerr << "Invalid device count: " << dev_cnt << endl;
      return;
    }
    // create
    MetricTablePerformance tp(metric_schema_, tag_schema_, "perf_test", "perf_metric");
    if (0 != tp.create(10086)) {
      cerr << "CreateMetricTable failed" << endl;
      return;
    }

    // insert
    size_t avg = dev_cnt / thr_cnt;
    size_t mod = dev_cnt % thr_cnt;
    vector<thread> workers;
    Timer tm;
    for (size_t i = 0; i < thr_cnt; ++i) {
      workers.emplace_back([this, avg, mod, row_cnt_per_dev, &tp] (size_t idx) {
        kwdbContext_t context;
        kwdbContext_p ctx = &context;
        size_t dev_cnt_per_thr = avg;
        if (idx < mod) {
          ++dev_cnt_per_thr;
        }
        for (size_t j = 0; j < dev_cnt_per_thr; ++j) {
          // device: tag
          auto tag_data = tpg_->construct(nullptr,
                                          string("hostname_") + std::to_string(j + idx * (mod == 0 ? avg : avg + 1)),
                                          string("region_") + std::to_string(j % 3),
                                          string("datacenter_") + std::to_string(j % 5),
                                          std::to_string(j % 100),
                                          string("Ubuntu-") + std::to_string((j % 6) * 2 + 10),
                                          j % 3 == 0 ? "x86" : (j % 3 == 1 ? "x64" : "aarch64"),
                                          j % 4 == 0 ? "NYC" : (j % 4 == 1 ? "CHI" : (j % 4 == 2 ? "LON" : "SF")),
                                          std::to_string(j % 29),
                                          std::to_string(j % 13),
                                          j % 7 == 1 ? "test" : (j % 7 == 3 ? "staging" : "production"));
          constexpr static size_t ROW_PER_INSERT = 1;  // insert `ROW` row everytime.
          auto now = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count();
          pg_->set_group(1);
          for (size_t k = 0; k < row_cnt_per_dev / ROW_PER_INSERT; ++k) {
            auto payload = pg_->construct(tag_data, tpg_->payload_size(), 2, 30, ROW_PER_INSERT,
                                          nullptr,
                                          now + k,
                                          j * k % 11,
                                          j * k % 101,
                                          j * k % 1001,
                                          j * k % 10001,
                                          j * k % 100001,
                                          j * k % 1000001,
                                          j * k % 10000001,
                                          j * k % 100000001,
                                          j * k % 1000000001,
                                          j * k % 10000000001);
            // insert
            tp.insert(ctx, payload.first, payload.second);
            PayloadGenerator::destroy(payload.first);
          }
          if (row_cnt_per_dev % ROW_PER_INSERT) {
            size_t last_size = row_cnt_per_dev / ROW_PER_INSERT;
            auto last_payload = pg_->construct(tag_data, tpg_->payload_size(), 2, 30,
                                               static_cast<int32_t>(row_cnt_per_dev % ROW_PER_INSERT),
                                               nullptr,
                                               now + last_size,
                                               j * last_size % 11,
                                               j * last_size % 101,
                                               j * last_size % 1001,
                                               j * last_size % 10001,
                                               j * last_size % 100001,
                                               j * last_size % 1000001,
                                               j * last_size % 10000001,
                                               j * last_size % 100000001,
                                               j * last_size % 1000000001,
                                               j * last_size % 10000000001);
            tp.insert(ctx, last_payload.first, last_payload.second);
            PayloadGenerator::destroy(last_payload.first);
          }
          TagPayloadGenerator::destroy(tag_data);
        }
      }, i);
    }
    // join
    for (auto & w :workers) { w.join(); }
    auto elapsed = tm.tick();
    print_statistics("INSERT", dev_cnt * row_cnt_per_dev, elapsed);
  }

 protected:
  vector<AttributeInfo> metric_schema_;
  vector<TagInfo> tag_schema_{TagSchemaCase<5>::schema()};
  std::shared_ptr<PayloadGenerator> pg_;
  std::shared_ptr<TagPayloadGenerator> tpg_;
};

class MetricQueryTest : public MetricInsertTest {
 public:
  void perf_tag_iterator(size_t dev_cnt, size_t thr_cnt) {
    cout << "************** Begin tag iterator traverse **************" << endl;
    // create
    MetricTablePerformance tp(metric_schema_, tag_schema_, "perf_test", "perf_metric");
    if (0 != tp.create(10086)) {
      cerr << "CreateMetricTable failed" << endl;
      return;
    }
    // insert
    kwdbContext_t context;
    kwdbContext_p ctx = &context;
    auto now = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count();
    for (size_t i = 0; i < dev_cnt; ++i) {
      auto tag_data = tpg_->construct(nullptr,
                                      string("hostname_") + std::to_string(i),
                                      string("region_") + std::to_string(i % 3),
                                      string("datacenter_") + std::to_string(i % 5),
                                      std::to_string(i % 100),
                                      string("Ubuntu-") + std::to_string((i % 6) * 2 + 10),
                                      i % 3 == 0 ? "x86" : (i % 3 == 1 ? "x64" : "aarch64"),
                                      i % 4 == 0 ? "NYC" : (i % 4 == 1 ? "CHI" : (i % 4 == 2 ? "LON" : "SF")),
                                      std::to_string(i % 29),
                                      std::to_string(i % 13),
                                      i % 7 == 1 ? "test" : (i % 7 == 3 ? "staging" : "production"));
      auto payload = pg_->construct(tag_data, tpg_->payload_size(), 2, 30, 1,
                                    nullptr,
                                    now++,
                                    11,
                                    101,
                                    1001,
                                    10001,
                                    100001,
                                    1000001,
                                    10000001,
                                    100000001,
                                    1000000001,
                                    10000000001);
      tp.insert(ctx, payload.first, payload.second);
      PayloadGenerator::destroy(payload.first);
      TagPayloadGenerator::destroy(tag_data);
    }
    // traverse
    vector<thread> workers;
    Timer tm;
    for (size_t i = 0; i < thr_cnt; ++i) {
      workers.emplace_back([&tp] (size_t idx) {
        kwdbContext_t context;
        kwdbContext_p ctx = &context;
        auto r = tp.traverse_tag_by_iterator(ctx);
        fprintf(stdout, "thread(%zu) return %zu\n", idx, r);
      }, i);
    }
    for (auto & w : workers) w.join();
    auto elapsed = tm.tick();
    print_statistics("TAG ITERATOR", dev_cnt, elapsed);
  }
};

TEST_P(MetricInsertTest, schema_case_5) {
  MetricTestParam param = GetParam();
  perf_insert(param.device_count, param.thread_count, param.row_count_per_dev);
}

TEST_P(MetricQueryTest, schema_case_5) {
  MetricTestParam param = GetParam();
  perf_tag_iterator(param.device_count, param.thread_count);
}