#pragma once
#include <gtest/gtest.h>
#include <random>
#include <chrono>
#include <fstream>
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif
#include <utility>
#include "utils/big_table_utils.h"
#include "mmap/mmap_tag_column_table.h"
#include "payload_generator.h"
#include "common.h"

class TagTablePerformance final {
 public:
  explicit TagTablePerformance(const vector<TagInfo>& schema, string home, string dbname)
      : schema_(schema),
        bt_(nullptr),
        db_home_(std::move(home)),
        db_name_(std::move(dbname)) {
  }

  ~TagTablePerformance() {
    if (bt_) {
      releaseObject(bt_);
      bt_ = nullptr;
    }
  }

  /**
   * @brief create tag table
   * @param table_id
   * @return
   */
  int create(uint64_t table_id) {
    ErrorInfo err_info;
    MMapTagColumnTable* bt = CreateTagTable(schema_, db_home_, db_name_,
                                                 table_id, 1, TAG_TABLE, err_info);
    if (bt) {
      bt_ = bt;
    }
    return err_info.errcode;
  }

  /**
   * @brief remove tag table
   * @return
   */
  int remove() {
    if (bt_) {
      auto r = bt_->remove();
      return r;
    }
    return 0;
  }

  /**
   * @brief insert payload into tag table
   * @param entity_id entity id
   * @param group_id group id
   * @param payload payload
   * @return error code
   */
  int insert(size_t entity_id, size_t group_id, void* payload) {
    if (!bt_) {
      cerr << "Invalid tag table instance" << endl;
      return -1;
    }

    auto r = bt_->insert(entity_id, group_id, reinterpret_cast<char*>(payload));
    return r;
  }

  [[nodiscard]] inline auto get() const -> MMapTagColumnTable* { return bt_; }

  int query(const vector<void *>& primary, const vector<uint32_t>& tag_col_idx) {
    if (!bt_) {
      cerr << "Invalid tag table instance" << endl;
      return -1;
    }

    // result
    vector<kwdbts::EntityResultIndex> entity_list;
    kwdbts::ResultSet rs;
    uint32_t cnt = 0;

    bt_->GetEntityIdList(primary, tag_col_idx, &entity_list, &rs, &cnt);

    if (tag_col_idx.empty() && entity_list.size() != cnt) {
      cerr << "Invalid result: count(entity)=" << entity_list.size()
           << ", cnt=" << cnt << endl;
      return -1;
    } else if (entity_list.size() != rs.data.begin()->second.size() || rs.data.begin()->second.size() != cnt) {
      cerr << "Invalid result: count(entity)=" << entity_list.size()
           << ", count(result)=" << rs.data.begin()->second.size()
           << ", cnt=" << cnt << endl;
      return -1;
    }
    return 0;
  }

 private:
  vector<TagInfo> schema_;
  MMapTagColumnTable* bt_;
  string db_home_;
  string db_name_;
};

template<size_t CASE>
class TagSchemaCase {
 public:
  /**
   * @brief schema of this case
   * @return schema
   */
  static const vector<TagInfo>& schema() {
    if (!payload_generator) {
      init();
    }
    return payload_generator->schema();
  }

  /**
   * @brief generate payload file for this case
   * @param row row count
   * @param filepath payload file path
   */
  static void gen_payload(size_t row, const char* filepath) {
    std::ofstream f(filepath, ios_base::app);
    if (!f.is_open()) {
      cerr << "Failed to open dump file: " << filepath << endl;
      return;
    }
    if (!payload_generator) {
      init();
    }
    generate(f, row);
    f.close();
  }

  /**
   * @brief length of one payload
   * @return payload length
   */
  static size_t payload_length() {
    if (!payload_generator) {
      init();
    }
    return payload_generator->payload_size();
  }

  static size_t primary_length() {
    if (!payload_generator) {
      init();
    }
    size_t r = 0;
    for (const auto & attr : payload_generator->schema()) {
      if (attr.m_tag_type != PRIMARY_TAG) {
        continue;
      }
      if (attr.m_data_type == DATATYPE::VARSTRING) {
        r += attr.m_length;
      } else {
        r += getDataTypeSize(attr.m_data_type);
      }
    }
    return r;
  }

 private:
  static std::shared_ptr<TagPayloadGenerator> payload_generator;

  // need to implement
  static void init();

  // need to implement
  static void generate(std::ofstream& f, size_t row);

  static void init(const vector<TagInfo>& s) {
    if (!payload_generator) {
      payload_generator = std::make_shared<TagPayloadGenerator>(s);
    }
  }
};

// TSBS datasource schema
template<> std::shared_ptr<TagPayloadGenerator> TagSchemaCase<5>::payload_generator = nullptr;

template<>
void TagSchemaCase<5>::init() {
  static vector<TagInfo> schema = {
    {0, DATATYPE::VARSTRING, 30, 0, 30, PRIMARY_TAG},
    {1, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {2, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {3, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {4, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {5, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {6, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {7, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {8, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
    {9, DATATYPE::VARSTRING, 30, 0, 30, GENERAL_TAG},
  };
  init(schema);
}

template<>
void TagSchemaCase<5>::generate(std::ofstream& f, size_t row) {
  for (size_t i = 0; i < row; ++i) {
    auto payload = payload_generator->construct(nullptr,
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
    // dump payload to file
    f.write((const char*) payload, static_cast<std::streamsize>(payload_generator->payload_size()));
    payload_generator->destroy(payload);
  }
}

/**
 * @brief load payload data to memory from payload file
 * @tparam CASE schema case, see: SchemaCase
 * @param row_count row count
 * @return payload data
 */
template<size_t CASE>
uint8_t* load_payload(size_t row_count, const string& filepath) {
  auto payload_len = TagSchemaCase<CASE>::payload_length();
  std::filesystem::path filename = filepath;
  ifstream ifs(filename);
  if (!ifs.is_open()) {
    cerr << "Cannot open file: " << filename << endl;
    return nullptr;
  }
  auto payload_data = new uint8_t[payload_len * row_count];
  ifs.read(reinterpret_cast<char*>(payload_data), payload_len * row_count);
  ifs.close();
  return payload_data;
}

/**
 * @brief unload payload data from memory
 * @tparam CASE schema case, see: SchemaCase
 * @param payload payload data ptr
 */
template<size_t CASE>
void unload_payload(const uint8_t* payload) {
  delete[] payload;
}

/////////////////////////////// test ///////////////////////////////

struct TagPerfParams {
  struct {
    size_t thread_count;
  } common;
  struct {
    size_t row_count_per_thread;
    // threshold of insert row per second. If the actual execution result is less than this value, the test case will fail.
    double threshold_min_rps;
  } insert;
  struct {
    size_t total_row_count;
    size_t total_query_count;
    // threshold of query time(us). If the actual execution result is larger than this value, the test case will fail.
    size_t threshold_max_query_us;
  } query;
};

// 1100w row
constexpr size_t MAX_ROW_COUNT = 11000000;

class TagInsertTest : public TestCommon, public ::testing::TestWithParam<TagPerfParams> {
 public:
  TagInsertTest() {
    db_name_ = "perf_tag";
    iot_interval_ = 3600;
  }

  ~TagInsertTest() override = default;

  // global
  static void SetUpTestCase() {
    cout << "generate payload..." << endl;
    // only generate schema case 5
    // gen_payload<0>(MAX_ROW_COUNT);
    // gen_payload<1>(MAX_ROW_COUNT);
    // gen_payload<2>(MAX_ROW_COUNT);
    // gen_payload<3>(MAX_ROW_COUNT);
    // gen_payload<4>(MAX_ROW_COUNT);
    gen_payload<5>(MAX_ROW_COUNT);
  }

  static void TearDownTestCase() {
    cout << "destroy payload..." << endl;
    // del_payload<0>(MAX_ROW_COUNT);
    // del_payload<1>(MAX_ROW_COUNT);
    // del_payload<2>(MAX_ROW_COUNT);
    // del_payload<3>(MAX_ROW_COUNT);
    // del_payload<4>(MAX_ROW_COUNT);
    // del_payload<5>(MAX_ROW_COUNT);
  }

  void SetUp() override {
    create_db();
  }

  void TearDown() override {}

 protected:

  static inline std::filesystem::path payload_path() { return {"./payload"}; }

  static inline std::filesystem::path payload_file_path(size_t schema_case, size_t row_count) {
    auto p = payload_path();
    p = p / ("PAYLOAD_SCHEMA_" + std::to_string(schema_case) + "_ROW_" + std::to_string(row_count) + ".bin");
    return p;
  }

 protected:
  /**
   * @brief generate payload file
   * @tparam CASE schema case, see: SchemaCase
   * @param row_count row count
   */
  template<size_t CASE>
  static void gen_payload(size_t row_count) {
    if (!exists(payload_path())) {
      std::filesystem::create_directories(payload_path());
    }
    // reuse payload file
    if (!exists(payload_file_path(CASE, row_count))) {
      TagSchemaCase<CASE>::gen_payload(row_count, payload_file_path(CASE, row_count).c_str());
    }
  }

  /**
   * @brief delete payload file
   * @tparam CASE schema case, see: SchemaCase
   * @param row_count row count
   */
  template<size_t CASE>
  [[maybe_unused]] static void del_payload(size_t row_count) {
    if (exists(payload_file_path(CASE, row_count))) {
      std::filesystem::remove_all(payload_file_path(CASE, row_count));
    }
  }

  /**
   * @brief test func for insert
   * @tparam SC SchemaCase
   * @param thread_cnt thread count
   * @param row_per_thr row count per-thread insert
   */
  template<size_t SC>
  double insert(size_t thread_cnt, size_t row_per_thr) {
    TagTablePerformance sn(TagSchemaCase<SC>::schema(), kw_home_, db_name_);

    // create tag table
    Timer tm;
    if (sn.create(::getpid())) {
      cerr << "create failed" << endl;
      return 0;
    }
    auto create_elapsed = tm.tick();

    // load payload
    auto payload = load_payload<SC>(thread_cnt * row_per_thr, payload_file_path(SC, MAX_ROW_COUNT));
    auto payload_len = TagSchemaCase<SC>::payload_length();
    if (!payload) {
      cerr << "Load payload failed" << endl;
      sn.remove();
      return 0;
    }

    // insert
    vector<thread> workers;
    tm.tick();
    for (size_t i = 0; i < thread_cnt; ++i) {
      workers.emplace_back([=, &sn] {
        for (size_t j = 0; j < row_per_thr; ++j) {
          auto entity = i * row_per_thr + j;
          sn.insert(entity, entity % 500, payload + (i * row_per_thr + j) * payload_len);
        }
      });
    }
    for (auto& w : workers) { w.join(); }
    auto insert_elapsed = tm.tick();

    // unload payload
    unload_payload<SC>(payload);

    // test print
    auto bt = sn.get();
    if (bt) {
      auto total = bt->size();
      cout << "############# total: " << total << ", last 10 records of tag table #############"
           << bt->printRecord(total - 9, total) << endl;
    }

    // remove
    tm.tick();
    sn.remove();
    auto remove_elapsed = tm.tick();

    // print statistics
    cout << "************ " << db_name_ << "." << ::getpid() << " ************" << endl;
    print_statistics("CREATE", 1, create_elapsed);
    print_statistics("REMOVE", 1, remove_elapsed);
    print_statistics("INSERT", thread_cnt * row_per_thr, insert_elapsed);
    return static_cast<double>(thread_cnt * row_per_thr) / (static_cast<double>(insert_elapsed) / 1000000);;
  }
};

class TagQueryTest : public TagInsertTest {
 protected:
  template <size_t SC>
  uint64_t query(size_t total_cnt, size_t qry_cnt, size_t thr_cnt, const vector<uint32_t>& tag_col_idx) {
    TagTablePerformance sn(TagSchemaCase<SC>::schema(), kw_home_, db_name_);

    // create tag table
    if (sn.create(::getpid())) {
      cerr << "create failed" << endl;
      return std::numeric_limits<uint64_t>::max();
    }

    // load payload
    auto payload = load_payload<SC>(total_cnt, payload_file_path(SC, MAX_ROW_COUNT));
    auto payload_len = TagSchemaCase<SC>::payload_length();
    if (!payload) {
      cerr << "Load payload failed" << endl;
      sn.remove();
      return std::numeric_limits<uint64_t>::max();
    }

    // save some primary
    vector<vector<void *>> primary_payloads(thr_cnt);
    auto bitmap_len = (TagSchemaCase<SC>::schema().size() + 7) >> 3;
    auto primary_len = TagSchemaCase<SC>::primary_length();

    cout << "///////////// primary_len = " << primary_len << endl;

    // insert, single thread
    size_t thr_idx = 0, assign = 0;
    for (size_t i = 0; i < total_cnt; ++i) {
      auto p = payload + i * payload_len;
      sn.insert(i, i % 500, p);
      if (qry_cnt != 0 && i % (total_cnt / qry_cnt) == 0 && assign++ < qry_cnt) {
        auto pl = new uint8_t [primary_len];
        memcpy(pl, p + bitmap_len, primary_len);
        primary_payloads[thr_idx++ % thr_cnt].emplace_back(pl);
      }
    }

    cout << "///////////// primary_payloads.size() = " << primary_payloads.size() << endl;
    for (const auto & p : primary_payloads) {
      cout << "///////////// primary_payloads[i].size() = " << p.size() << endl;
    }

    // unload payload
    unload_payload<SC>(payload);

    // query
    vector<thread> workers;
    Timer tm;
    for (size_t i = 0; i < thr_cnt; ++i) {
      workers.emplace_back([qry_cnt, &sn, &tag_col_idx, &primary_payloads] (size_t thr_idx) {
        if (qry_cnt == 0) {
          // full scan
          return sn.query({}, tag_col_idx);
        }
        if (qry_cnt == 1) {
          return sn.query(primary_payloads[0], tag_col_idx);
        }
        return sn.query(primary_payloads[thr_idx], tag_col_idx);
      }, i);
    }
    for (auto & w : workers) { w.join(); }
    auto elapsed = tm.tick();

    // deallocate
    for (auto & pp : primary_payloads) {
      for (auto p : pp) {
        delete[] reinterpret_cast<uint8_t *>(p);
      }
    }

    // remove
    sn.remove();

    // print statistics
    cout << "************ " << db_name_ << "." << ::getpid() << " ************" << endl;
    print_statistics("QUERY", qry_cnt ? qry_cnt : total_cnt, elapsed);
    return elapsed;
  }
};

class TagFullScanTest : public TagQueryTest {};

///////////////////////// test case /////////////////////////

TEST_P(TagInsertTest, insert_schema_5) {
  auto param = GetParam();
  auto rps = insert<5>(param.common.thread_count, param.insert.row_count_per_thread);
  EXPECT_GE(rps, param.insert.threshold_min_rps);
}

TEST_P(TagQueryTest, query_schema_5) {
  auto param = GetParam();
  vector<uint32_t> tag_idx;
  for (uint32_t i = 0; i < TagSchemaCase<5>::schema().size(); ++i) { tag_idx.emplace_back(i); }
  auto duration = query<5>(param.query.total_row_count, param.query.total_query_count,
                           param.common.thread_count, tag_idx);
  EXPECT_LE(duration, param.query.threshold_max_query_us);
}

TEST_P(TagFullScanTest, full_scan_schema_5) {
  auto param = GetParam();
  vector<uint32_t> tag_idx;
  for (uint32_t i = 0; i < TagSchemaCase<5>::schema().size(); ++i) { tag_idx.emplace_back(i); }
  auto duration = query<5>(param.query.total_row_count, 0,
                           param.common.thread_count, tag_idx);
  EXPECT_LE(duration, param.query.threshold_max_query_us);
}
