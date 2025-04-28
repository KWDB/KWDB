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

#include <gtest/gtest.h>

#include "utils/date_time_util.h"
#include "st_config.h"
#include "ts_time_partition.h"
#include "test_util.h"

const string TestBigTableInstance::kw_home_ = "./data_kw/";  // NOLINT
const string TestBigTableInstance::db_name_ = "testdb";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestPartitionBigTable : public TestBigTableInstance {
 public:
  TestPartitionBigTable() : db_name_(TestBigTableInstance::db_name_), db_path_(TestBigTableInstance::kw_home_) {
    // Clean up the directory
    system(("rm -rf " + TestBigTableInstance::kw_home_ + "/*").c_str());
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
  }

  std::string db_name_;
  std::string db_path_;
  kwdbts::kwdbContext_t context_;
  kwdbts::kwdbContext_p ctx_;

  void IsDiscard(size_t idx, bool *is_discard, char* data) {
    size_t byte = idx >> 3;
    size_t bit = 1 << (idx & 7);
    *is_discard = data[byte] & bit;
  }

 protected:
  static void setUpTestCase() {}

  static void tearDownTestCase() {}
};

/*
 * @brief Test the basic functions of MMapEntityMeta
 */
TEST_F(TestPartitionBigTable, base) {
  EntityBlockMetaManager meta_manager;
  // Create a partition directory with specific permissions.
  std::string pt_tbl_sub_path = "20231110/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  int error_code = meta_manager.Open("t1.meta", db_path_, pt_tbl_sub_path, true);
  EXPECT_EQ(error_code, 0);

  // Assign and create block items for each entity
  size_t entity_group_max = 100;
  for (int i = 1; i <= entity_group_max; i++) {
    EntityItem* entity_item = meta_manager.getEntityItem(i);
    entity_item->entity_id = i;
    entity_item->row_written = i + 1000;
    entity_item->cur_block_id = 0;
    BlockItem* block_item = nullptr;
    meta_manager.AddBlockItem(i, &block_item);
    meta_manager.UpdateEntityItem(i, block_item);

    ASSERT_EQ(block_item->entity_id, i);
    ASSERT_EQ(meta_manager.getEntityItem(i)->cur_block_id, i);
  }

  // First entity appends 10 block items
  int entity_id = 1;
  for (int i = 1; i <= 10; i++) {
    BlockItem* block_item = nullptr;
    meta_manager.AddBlockItem(entity_id, &block_item);
    meta_manager.UpdateEntityItem(entity_id, block_item);
    EXPECT_EQ(meta_manager.getEntityItem(entity_id)->cur_block_id, entity_group_max + i);
  }
  // Get the result
  meta_manager.getEntityItem(entity_id)->to_string(std::cout);
}

/*
 * @brief Test the simple write function of MMapPartitionTable
 */
TEST_F(TestPartitionBigTable, insert) {
  // Initialize an error information object.
  ErrorInfo err_info;
  // Define the table name and table type.
  string table_name = "t1";
  TableType table_type = TableType::ENTITY_TABLE;
  // Create the root table.
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  // Verify that no error occurred during table creation.
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);

  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  // Create a partition directory with specific permissions.
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  // Declare a vector to hold keys, though it remains unused in this snippet.
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);

  // Insert a batch of data with a starting entity ID of 1, consisting of 10 rows,
  // using the current time as the base timestamp for data validity.
  uint32_t entity_id = 1;
  uint32_t row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, row_num, &dedup_result);
  // Validate that the inserted rows match the expected count for entity ID 1.
  ASSERT_EQ(countRecords(mt_table, entity_id), row_num);
  // Further validations on block items and their contents follow, ensuring correct data storage and aggregation.
  // Repeat the insertion process for a different entity ID (2), this time with 17 rows,
  // and validate the stored data integrity as before.
  entity_id = 2;
  row_num = 17;
  ts_now = time(nullptr) * 1000;
  initData2(ctx_, mt_table, entity_id, ts_now, row_num, &dedup_result);
  ASSERT_EQ(countRecords(mt_table, entity_id), 17);
  // Additional checks to confirm the second batch of data was correctly handled.

  // Clean up allocated resources.
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Tests the functionality of writing out-of-order data to MMapEntityBigTable with default retention.
 */
TEST_F(TestPartitionBigTable, disorder) {
  ErrorInfo err_info;  // Initialize error tracking information
  string table_name = "t2";  // Define the test table name
  TableType table_type = TableType::ENTITY_TABLE;  // Specify the table type as ENTITY_TABLE
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  // Create the root table and ensure no errors occurred during creation
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  // Create the partition directory with appropriate permissions
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  // Instantiate a Partition Table with a specified entity capacity
  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  // Set up for data insertion: entity ID, timestamp, number of rows, and initialize deduplication result
  uint32_t entity_id = 1;
  int row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;  // Set initial timestamp
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, row_num, &dedup_result);  // Insert ordered data

  // Validate the number of rows inserted for the given entity ID
  EXPECT_EQ(countRecords(mt_table, entity_id), row_num);
  // Retrieve and validate properties of the first block item
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, row_num);

  // Fetch associated Segment Table and ensure it's not null
  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);

  // Validate the minimum timestamp of the block matches the initial timestamp
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  // Insert a single out-of-order data point and revalidate expectations
  ts_now = time(nullptr) * 1000;
  initData2(ctx_, mt_table, entity_id, ts_now, 1, &dedup_result);
  EXPECT_EQ(countRecords(mt_table, entity_id), row_num + 1);
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  // Clean up allocated resources
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the override duplication feature of MMapEntityBigTable.
 */
TEST_F(TestPartitionBigTable, override) {
  ErrorInfo err_info;
  string table_name = "t2";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  // Configuration for data insertion: Entity ID, current timestamp, number of rows, duplication rule
  uint32_t entity_id = 1;
  int row_count = 10;
  timestamp64 current_time_ms = time(nullptr) * 1000 + 1000000;
  DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};

  // Perform initial data population with override duplication rule
  initData2(ctx_, mt_table, entity_id, current_time_ms, row_count, &dedup_result, kwdbts::DedupRule::OVERRIDE);

  // Validate the size of data for the entity and block item details
  EXPECT_EQ(countRecords(mt_table, entity_id), row_count);
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, row_count);

  // Fetch and validate the segment table associated with the block
  std::shared_ptr<MMapSegmentTable> segment_table = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_table, nullptr);
  ASSERT_EQ(block_item->min_ts_in_block, current_time_ms);
  ASSERT_EQ(KInt16(segment_table->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 10);

  // Check that no rows are marked as deleted before introducing new data
  bool is_deleted = false;
  for (int i = 0; i < row_count; ++i) {
    block_item->isDeleted(10, &is_deleted);
    ASSERT_FALSE(is_deleted);
  }

  // Introduce another set of data with override rule, causing duplication and potential overrides
  initData2(ctx_, mt_table, entity_id, current_time_ms, row_count, &dedup_result, kwdbts::DedupRule::OVERRIDE);

  // Verify that original rows are now marked as deleted and total size reflects the data
  for (int i = 0; i < row_count; ++i) {
    block_item->isDeleted(i + 1, &is_deleted);
    ASSERT_TRUE(is_deleted);
  }
  ASSERT_EQ(countRecords(mt_table, entity_id), row_count);
  ASSERT_EQ(block_item->min_ts_in_block, current_time_ms);
  // Also verify aggregation result on a specific column
  ASSERT_EQ(KInt16(segment_table->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 20);

  // Clean up allocated resources
  delete mt_table;
  delete root_bt_manager;
}

/*
 * Test the de-duplication functionality of MMapEntityBigTable with the REJECT policy.
 */
TEST_F(TestPartitionBigTable, reject) {
  // Initialize error information
  ErrorInfo err_info;
  std::string table_name = "t2";  // Table name for testing
  TableType table_type = TableType::ENTITY_TABLE;  // Type of the table being tested

  // Create the root metrics table
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  // Create the partition directory
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);  // Set permissions

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  // Setup: Entity ID starts at 1, write 10 sequential data entries with current timestamp
  uint32_t entity_id = 1;
  int row_num = 10;
  // Current timestamp in milliseconds
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  // Initial de-duplication result state
  DedupResult dedup_result{2, 0, 0, TSSlice {nullptr, 0}};
  // Perform initial data insertion
  initData2(ctx_, mt_table, entity_id, ts_now, row_num, &dedup_result, kwdbts::DedupRule::REJECT);

  // Validate the de-duplication result after first batch insertion
  ASSERT_EQ(dedup_result.dedup_rule, 2);  // Expected de-duplication rule applied
  ASSERT_EQ(dedup_result.dedup_rows, 0);  // No rows were de-duplicated yet
  ASSERT_EQ(dedup_result.discard_bitmap.len, 0);  // Discard bitmap is empty
  ASSERT_EQ(dedup_result.discard_bitmap.data, nullptr);  // Bitmap pointer is null
  EXPECT_EQ(countRecords(mt_table, entity_id), row_num);  // Correct number of rows inserted

  // Further validations on block structure and content
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, row_num);  // Row count matches in block item

  // Validate segment table existence and properties
  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);  // Segment table exists

  ASSERT_EQ(block_item->min_ts_in_block, ts_now);  // Timestamp validation
  bool is_deleted = false;
  for (int i = 0; i < row_num; i++) {
    block_item->isDeleted(i + 1, &is_deleted);  // Verify no rows are marked as deleted
    ASSERT_FALSE(is_deleted);
  }

  // Second phase: Insert another 10 out-of-order data entries, validating rejection behavior
  initData2(ctx_, mt_table, entity_id, ts_now, 10, &dedup_result, kwdbts::DedupRule::REJECT);

  // Confirm deletion flags post-second insertion (should remain unchanged due to REJECT strategy)
  for (int i = 0; i < row_num; i++) {
    block_item->isDeleted(i + 1, &is_deleted);
    ASSERT_FALSE(is_deleted);
  }

  // Final checks on de-duplication results and table size after second insertion
  ASSERT_EQ(dedup_result.dedup_rule, 2);
  ASSERT_EQ(dedup_result.dedup_rows, 10);  // Rows rejected due to duplicates
  ASSERT_EQ(dedup_result.discard_bitmap.len, 0);
  ASSERT_EQ(dedup_result.discard_bitmap.data, nullptr);
  ASSERT_EQ(countRecords(mt_table, entity_id), 2 * row_num);  // Total rows as expected
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);  // Timestamp consistency

  // Clean up resources
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the duplicate (discard) functionality of MMapEntityBigTable.
 * Utilizes MMapPartitionTable for testing, encompassing data insertion, duplication handling,
 * and validation of the discard operation's effects on stored records.
 */
TEST_F(TestPartitionBigTable, discard) {
  ErrorInfo err_info;
  string table_name = "t2"; // Table name for testing
  TableType table_type = TableType::ENTITY_TABLE; // Table type as entity table
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  // Initialize test scenario: entity_id starts at 1, insert 10 sequential rows
  uint32_t entity_id = 1;
  int row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000; // Current timestamp for data
  // Initialization of deduplication result
  DedupResult dedup_result{3, 0, 1, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, row_num, &dedup_result, kwdbts::DedupRule::DISCARD);
  // Assertions to validate discard rule application and resultant state
  ASSERT_EQ(dedup_result.dedup_rule, 3);
  ASSERT_EQ(dedup_result.dedup_rows, 0);
  ASSERT_EQ(dedup_result.discard_bitmap.len, 0);
  ASSERT_EQ(dedup_result.discard_bitmap.data, nullptr);

  // Validate table size and block item details post-insertion
  EXPECT_EQ(countRecords(mt_table, entity_id), row_num);
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, row_num);

  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr); // Ensure segment table is properly initialized

  // Validate minimum timestamp and deletion status checks
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);
  bool is_deleted = false;
  for (int i = 0; i < row_num; i++) {
    block_item->isDeleted(i + 1, &is_deleted);
    ASSERT_FALSE(is_deleted); // Initial records should not be marked as deleted
  }
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the merge function of MMapEntityBigTable for deduplication
 */
TEST_F(TestPartitionBigTable, merge) {
  ErrorInfo err_info;
  std::string table_name = "t2";
  TableType table_type = TableType::ENTITY_TABLE;
  // Create the root table utilizing the provided database name and path, table name, type, with error tracking.
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  // Set the initial entity ID as 1, then sequentially insert 10 rows of data with the current timestamp.
  uint32_t entity_id = 1;
  int row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, row_num, &dedup_result, kwdbts::DedupRule::MERGE);

  // Validate that the table size matches the number of inserted rows for the given entity ID.
  EXPECT_EQ(countRecords(mt_table, entity_id), row_num);
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, row_num);

  // Retrieve the associated segment table and confirm it is not a null pointer.
  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);

  // Verify the minimum timestamp of the block matches the initial timestamp set for data insertion.
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  // Insert another 10 rows of data in a non-sequential order, maintaining the merge deduplication rule.
  initDataWithNull(ctx_, mt_table, entity_id, ts_now, 10, &dedup_result, kwdbts::DedupRule::MERGE);

  // Validate the aggregate values and counts post merging of sequential and non-sequential data.
  for (uint32_t i = 0; i < 2 * row_num; i++) {
    MetricRowID row{1, i+1};
    ASSERT_EQ(KInt64(segment_tbl->columnAddr(row, 1)), 11);
  }
  ASSERT_EQ(countRecords(mt_table, entity_id), row_num);
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 20);

  // Clean up allocated resources.
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the MMapEntityBigTable variable length data deduplication function merge
 */
TEST_F(TestPartitionBigTable, varColumnMerge) {
  ErrorInfo err_info;
  string table_name = "t2";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateRootTableVarCol(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  // Set the current entity id to 1 and write 10 pieces of data in order
  uint32_t entity_id = 1;
  int row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, row_num, &dedup_result, kwdbts::DedupRule::MERGE);

  EXPECT_EQ(countRecords(mt_table, entity_id), row_num);
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, row_num);

  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);

  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  // Write 10 pieces of data in random order
  initDataWithNull(ctx_, mt_table, entity_id, ts_now, 10, &dedup_result, kwdbts::DedupRule::MERGE);
  for (uint32_t i = 0; i < 2 * row_num; i++) {
    MetricRowID row{1, i+1};
    ASSERT_EQ(KInt64(segment_tbl->columnAddr(row, 1)), 11);
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    char aa[27];
    auto var_data = segment_tbl->varColumnAddr(row, 2);
    memcpy(aa, (char*)var_data.get() + sizeof(uint16_t), 27);
    ASSERT_EQ(strcmp(aa, test_str.data()), 0);
  }

  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test for switching deduplication modes in MMapEntityBigTable
 */
TEST_F(TestPartitionBigTable, keepToMerge) {
  ErrorInfo err_info;
  string table_name = "t2";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  // Set initial entity ID and sequentially write 10 records
  uint32_t entity_id = 1;
  uint32_t row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  for (int i = 0; i < row_num; i++) {
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    // Initialize data with KEEP deduplication rule
    initData2(ctx_, mt_table, entity_id, ts_now, 1, &dedup_result, kwdbts::DedupRule::KEEP, i * row_num);
  }
  // Validate that 10 rows have been written for the specified entity ID
  EXPECT_EQ(countRecords(mt_table, entity_id), row_num);
  // Retrieve block information and validate its publish row count
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, row_num);
  // Obtain the segment table and ensure it's not null
  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);
  // Confirm the minimum timestamp of the block matches the initial write timestamp
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  // Write a duplicate record with MERGE deduplication rule
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initDataWithNull(ctx_, mt_table, entity_id, ts_now, 1, &dedup_result, kwdbts::DedupRule::MERGE);
  // Validate data merge by checking the updated column value
  MetricRowID row{1, row_num + 1};
  ASSERT_EQ(KInt64(segment_tbl->columnAddr(row, 1)), row_num * (row_num - 1));
  // Validate the total row count after merging
  ASSERT_EQ(countRecords(mt_table, entity_id), 1);
  // Ensure the minimum timestamp remains unchanged after merge
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the ability of MMapEntityBigTable to write a large amount of data
 */
TEST_F(TestPartitionBigTable, bigdata) {
  ErrorInfo err_info;
  string table_name = "t3";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateRootTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  size_t block_item_row_max = 1000;
  int count = 100 * block_item_row_max;
  // Set the current entity id to 1, and write data for 10 block items in sequence
  uint32_t entity_id = 1;
  timestamp64 ts_now = time(nullptr) * 1000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, count, &dedup_result);

  EXPECT_EQ(countRecords(mt_table, entity_id), count);
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, 1000);
  ASSERT_EQ(block_item->is_agg_res_available, true);
  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);

  ASSERT_EQ(block_item->min_ts_in_block, ts_now);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 1000);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::COUNT)), 1000);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::MIN)), 11);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::MAX)), 11);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::SUM)), 11000);
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the UndoPut function of MMapPartitionBigTable
 */
TEST_F(TestPartitionBigTable, undoPut) {
  TS_LSN lsn = 100;
  ErrorInfo err_info;
  string table_name = "t1";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateLsnTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);

  // Set the current entity id to 1 and write 100 pieces of data
  uint32_t entity_id = 1;
  uint32_t row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, 100, &dedup_result);

  uint32_t payload_len = 0;
  std::vector<BlockSpan> cur_alloc_spans;
  std::vector<MetricRowID> to_del_rows;
  char* data = BtUtil::GenSomePayloadData(mt_table->getSchemaInfoIncludeDropped(), mt_table->getColsIdxExcludeDropped(), row_num, payload_len, ts_now + 10000, false);
  kwdbts::Payload pd(root_bt_manager, {data, payload_len});
  pd.SetLsn(lsn);
  uint32_t inc_unordered_cnt;
  pd.dedup_rule_ = kwdbts::DedupRule::KEEP;
  mt_table->push_back_payload(ctx_, entity_id, &pd, 0, row_num, &cur_alloc_spans, &to_del_rows, err_info, &inc_unordered_cnt, &dedup_result);
  ASSERT_TRUE(err_info.errcode >= 0);
  mt_table->publish_payload_space(cur_alloc_spans, to_del_rows, entity_id, true);

  // Verify whether the data is written correctly
  ASSERT_EQ(countRecords(mt_table, entity_id), row_num + 100);
  auto entity_item = mt_table->getEntityItem(entity_id);
  auto block_item = mt_table->GetBlockItem(entity_item->cur_block_id);

  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);

  ASSERT_EQ(block_item->publish_row_count, row_num + 100);
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  ASSERT_EQ(KInt64(segment_tbl->columnAddr(MetricRowID{1, 1}, 1)), 11);
  ASSERT_EQ(KDouble64(segment_tbl->columnAddr(MetricRowID{1, 1}, 2)), 2222.2);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 110);

  // Perform RedoPut
  std::vector<MetricRowID> to_deleted_real_rows;
  std::unordered_map<KTimestamp, MetricRowID> partition_ts_map;
  err_info.errcode = mt_table->RedoPut(ctx_, entity_id, lsn, 0, pd.GetRowCount(), &pd, &cur_alloc_spans,
                                       &to_deleted_real_rows, &partition_ts_map, 0, err_info);
  // Verify whether the data is written correctly
  ASSERT_EQ(countRecords(mt_table, entity_id), row_num + 100);
  ASSERT_EQ(block_item->publish_row_count, row_num + 100);
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  err_info.errcode = mt_table->UndoPut(entity_id, lsn, 0, pd.GetRowCount(), &pd, err_info);
  ASSERT_EQ(err_info.errcode, 0);
  // Verify whether the data can be undone
  ASSERT_EQ(countRecords(mt_table, entity_id), 100);
  ASSERT_EQ(block_item->publish_row_count, row_num + 100);

  ASSERT_EQ(KInt64(segment_tbl->columnAddr(MetricRowID{1, 1}, 1)), 11);
  ASSERT_EQ(KDouble64(segment_tbl->columnAddr(MetricRowID{1, 1}, 2)), 2222.2);

  // Continue to write data
  initData2(ctx_, mt_table, entity_id, ts_now + 100000, 100, &dedup_result);
  ASSERT_EQ(countRecords(mt_table, entity_id), 200);

  delete[]data;
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the MMapPartitionBigTable RedoPut function
 */
TEST_F(TestPartitionBigTable, redoPut) {
  TS_LSN lsn = 100;
  ErrorInfo err_info;
  string table_name = "t1";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateLsnTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);

  // 设置当前entity id为1，写入100条数据
  uint32_t entity_id = 1;
  uint32_t row_num = 10;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, 100, &dedup_result);

  uint32_t payload_len = 0;
  std::vector<BlockSpan> cur_alloc_spans;
  std::vector<MetricRowID> to_del_rows;
  char* data = BtUtil::GenSomePayloadData(mt_table->getSchemaInfoIncludeDropped(), mt_table->getColsIdxExcludeDropped(), row_num, payload_len, ts_now + 10000, false);
  kwdbts::Payload pd(root_bt_manager, {data, payload_len});
  pd.SetLsn(lsn);
  uint32_t inc_unordered_cnt;
  pd.dedup_rule_ = kwdbts::DedupRule::KEEP;
  mt_table->push_back_payload(ctx_, entity_id, &pd, 0, row_num, &cur_alloc_spans, &to_del_rows, err_info, &inc_unordered_cnt, &dedup_result);
  if (err_info.errcode >= 0) {
    mt_table->publish_payload_space(cur_alloc_spans, to_del_rows, entity_id, true);
  }
  ASSERT_EQ(countRecords(mt_table, entity_id), row_num + 100);
  auto entity_item = mt_table->getEntityItem(entity_id);
  auto block_item = mt_table->GetBlockItem(entity_item->cur_block_id);

  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);

  ASSERT_EQ(block_item->publish_row_count, row_num + 100);
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  ASSERT_EQ(KInt64(segment_tbl->columnAddr(MetricRowID{1, 1}, 1)), 11);
  ASSERT_EQ(KDouble64(segment_tbl->columnAddr(MetricRowID{1, 1}, 2)), 2222.2);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 110);

  std::vector<MetricRowID> to_deleted_real_rows;
  std::unordered_map<KTimestamp, MetricRowID> partition_ts_map;
  err_info.errcode = mt_table->RedoPut(ctx_, entity_id, lsn, 0, pd.GetRowCount(), &pd, &cur_alloc_spans,
                                       &to_deleted_real_rows, &partition_ts_map, 0, err_info);
  ASSERT_EQ(countRecords(mt_table, entity_id), row_num + 100);
  ASSERT_EQ(block_item->publish_row_count, row_num + 100);
  ASSERT_EQ(block_item->min_ts_in_block, ts_now);

  ASSERT_EQ(KInt64(segment_tbl->columnAddr(MetricRowID{1, 1}, 1)), 11);
  ASSERT_EQ(KDouble64(segment_tbl->columnAddr(MetricRowID{1, 1}, 2)), 2222.2);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 110);

  initData2(ctx_, mt_table, entity_id, ts_now + 100000, 100, &dedup_result);
  ASSERT_EQ(countRecords(mt_table, entity_id), 210);

  delete[]data;
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the updateAggregateResult function
 */
TEST_F(TestPartitionBigTable, aggUpdate) {
  ErrorInfo err_info;
  string table_name = "t3";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateRootTableVarCol(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  int count = 100;
  // Set the current entity id to 1, and write data for 10 block items in sequence
  uint32_t entity_id = 1;
  timestamp64 ts_now = time(nullptr) * 1000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initData2(ctx_, mt_table, entity_id, ts_now, count, &dedup_result, kwdbts::DedupRule::OVERRIDE);

  EXPECT_EQ(countRecords(mt_table, entity_id), count);
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, 100);
  ASSERT_EQ(block_item->is_agg_res_available, false);
  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 100);


  initData2(ctx_, mt_table, entity_id, ts_now, count * 10 , &dedup_result, kwdbts::DedupRule::OVERRIDE);

  bool is_deleted = false;
  for (int i = 0; i < count; ++i) {
    block_item->isDeleted(i + 1, &is_deleted);
    ASSERT_TRUE(is_deleted);
  }
  EXPECT_EQ(countRecords(mt_table, entity_id), count * 10);
  ASSERT_EQ(block_item->publish_row_count, 1000);
  ASSERT_EQ(block_item->is_agg_res_available, true);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 900);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::COUNT)), 900);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::MIN)), 11);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::MAX)), 11);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(1, 1, kwdbts::Sumfunctype::SUM)), 9900);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 2, kwdbts::Sumfunctype::COUNT)), 900);
  MetricRowID row{1, 1};
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  char aa[27];
  auto var_data = segment_tbl->varColumnAggAddr(row, 2, kwdbts::Sumfunctype::MAX);
  memcpy(aa, (char*)var_data.get() + sizeof(uint16_t), 27);
  ASSERT_EQ(strcmp(aa, test_str.data()), 0);

  auto block_item2 = mt_table->GetBlockItem(2);
  ASSERT_EQ(block_item2->publish_row_count, 100);
  ASSERT_EQ(block_item2->is_agg_res_available, false);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(2, 0, kwdbts::Sumfunctype::COUNT)), 100);
  ASSERT_EQ(block_item2->is_agg_res_available, false);
  delete mt_table;
  delete root_bt_manager;
}

TEST_F(TestPartitionBigTable, disorderPut) {
  TS_LSN lsn = 100;
  ErrorInfo err_info;
  string table_name = "t1";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateLsnTable(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  uint32_t entity_id = 1;
  uint32_t row_num = 1;
  timestamp64 ts_now = time(nullptr) * 1000 + 1000000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};

  uint32_t payload_len = 0;
  std::vector<BlockSpan> cur_alloc_spans;
  std::vector<MetricRowID> to_del_rows;
  char* data = BtUtil::GenSomePayloadData(mt_table->getSchemaInfoExcludeDropped(), mt_table->getColsIdxExcludeDropped(), row_num, payload_len, ts_now + 10000, false);
  kwdbts::Payload pd(root_bt_manager, {data, payload_len});
  pd.SetLsn(lsn);
  pd.dedup_rule_ = kwdbts::DedupRule::KEEP;
  for (int i = 0; i < 2; i++) {
    uint32_t inc_unordered_cnt;
    mt_table->push_back_payload(ctx_, entity_id, &pd, 0, row_num, &cur_alloc_spans, &to_del_rows, err_info, &inc_unordered_cnt, &dedup_result);
    if (err_info.errcode >= 0) {
      mt_table->publish_payload_space(cur_alloc_spans, to_del_rows, entity_id, true);
    }
  }
  ASSERT_EQ(countRecords(mt_table, entity_id), row_num * 2);
  auto entity_item = mt_table->getEntityItem(entity_id);
  ASSERT_FALSE(entity_item->is_disordered);
  // insert disorder data
  char* disorder_data = BtUtil::GenSomePayloadData(mt_table->getSchemaInfoExcludeDropped(), mt_table->getColsIdxExcludeDropped(), row_num, payload_len, ts_now, false);
  kwdbts::Payload pd_disorder(root_bt_manager, {disorder_data, payload_len});
  pd_disorder.SetLsn(lsn);
  uint32_t inc_unordered_cnt;
  pd_disorder.dedup_rule_ = kwdbts::DedupRule::KEEP;
  mt_table->push_back_payload(ctx_, entity_id, &pd_disorder, 0, row_num, &cur_alloc_spans, &to_del_rows, err_info, &inc_unordered_cnt, &dedup_result);
  if (err_info.errcode >= 0) {
    mt_table->publish_payload_space(cur_alloc_spans, to_del_rows, entity_id, true);
  }
  ASSERT_TRUE(entity_item->is_disordered);

  delete[] data;
  delete[] disorder_data;
  delete mt_table;
  delete root_bt_manager;
}

/*
 * @brief Test the updateAggregateResult function of null value
 */
TEST_F(TestPartitionBigTable, aggUpdateNull) {
  CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 10;
  ErrorInfo err_info;
  string table_name = "t3";
  TableType table_type = TableType::ENTITY_TABLE;
  MMapMetricsTable* root_bt = BtUtil::CreateRootTableVarCol(db_name_, db_path_, table_name, table_type, err_info);
  ASSERT_EQ(err_info.errmsg, "");
  MMapRootTableManager* root_bt_manager = new MMapRootTableManager(db_path_, db_name_, 123456789);
  root_bt_manager->PutTable(1, root_bt);
  string pt_tbl_sub_path = std::to_string(time(nullptr)) + "/";
  ::mkdir((db_path_ + pt_tbl_sub_path).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_bt_manager, CLUSTER_SETTING_MAX_ENTITIES_PER_SUBGROUP);
  mt_table->open(root_bt->name() + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  EXPECT_EQ(err_info.errmsg, "");

  int count = 10;
  // Set the current entity id to 1, and write data for 10 block items in sequence
  uint32_t entity_id = 1;
  timestamp64 ts_now = time(nullptr) * 1000;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  initDataWithNull(ctx_, mt_table, entity_id, ts_now, count, &dedup_result, kwdbts::DedupRule::OVERRIDE);

  EXPECT_EQ(countRecords(mt_table, entity_id), count);
  auto block_item = mt_table->GetBlockItem(1);
  ASSERT_EQ(block_item->publish_row_count, 10);
  ASSERT_EQ(block_item->is_agg_res_available, false);
  std::shared_ptr<MMapSegmentTable> segment_tbl = mt_table->getSegmentTable(block_item->block_id);
  ASSERT_NE(segment_tbl, nullptr);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(1, 0, kwdbts::Sumfunctype::COUNT)), 10);

  initData2(ctx_, mt_table, entity_id, ts_now, count , &dedup_result, kwdbts::DedupRule::OVERRIDE);

  bool is_deleted = false;
  for (int i = 0; i < count; ++i) {
    block_item->isDeleted(i + 1, &is_deleted);
    ASSERT_TRUE(is_deleted);
  }
  EXPECT_EQ(countRecords(mt_table, entity_id), count);
  ASSERT_EQ(block_item->publish_row_count, 10);
  ASSERT_EQ(block_item->is_agg_res_available, false);

  auto block_item2 = mt_table->GetBlockItem(2);
  ASSERT_EQ(block_item2->publish_row_count, 10);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(2, 0, kwdbts::Sumfunctype::COUNT)), 10);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(2, 1, kwdbts::Sumfunctype::COUNT)), 10);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(2, 1, kwdbts::Sumfunctype::MIN)), 11);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(2, 1, kwdbts::Sumfunctype::MAX)), 11);
  ASSERT_EQ(KInt64(segment_tbl->columnAggAddr(2, 1, kwdbts::Sumfunctype::SUM)), 110);
  ASSERT_EQ(KInt16(segment_tbl->columnAggAddr(2, 2, kwdbts::Sumfunctype::COUNT)), 10);
  MetricRowID row{2, 1};
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  char aa[27];
  auto var_data = segment_tbl->varColumnAggAddr(row, 2, kwdbts::Sumfunctype::MAX);
  memcpy(aa, (char*)var_data.get() + sizeof(uint16_t), 27);
  ASSERT_EQ(strcmp(aa, test_str.data()), 0);
  ASSERT_EQ(block_item2->is_agg_res_available, true);
  delete mt_table;
  delete root_bt_manager;
}