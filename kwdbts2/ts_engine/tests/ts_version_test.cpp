#include "ts_version.h"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <string_view>

#include "data_type.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_entity_segment.h"
#include "ts_entity_segment_data.h"
#include "ts_entity_segment_handle.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_lastsegment_builder.h"
#include "ts_segment.h"

class TsVersionTest : public testing::Test {
 protected:
  TsIOEnv *env = &TsIOEnv::GetInstance();
  fs::path vgroup_root = "version_test";

  KwTsSpan all_data{INT64_MIN, INT64_MAX};

  void SetUp() override {
    env->DeleteDir(vgroup_root);
    env->NewDirectory(vgroup_root);
  }

  void BrandNewEntitySegment(TsVersionManager *mgr, const fs::path &root, EntitySegmentMetaInfo *info) {
    {
      info->header_b_info.file_number = mgr->NewFileNumber();
      auto header_b_filename = root / BlockHeaderFileName(info->header_b_info.file_number);
      std::unique_ptr<TsAppendOnlyFile> file_b;
      ASSERT_EQ(env->NewAppendOnlyFile(header_b_filename, &file_b), SUCCESS);
      TsBlockItemFileHeader header_b;
      memset(&header_b, 0, sizeof(TsBlockItemFileHeader));
      header_b.status = TsFileStatus::READY;
      header_b.magic = TS_ENTITY_SEGMENT_BLOCK_ITEM_FILE_MAGIC;
      file_b->Append(TSSlice{(char *)&header_b, sizeof(TsBlockItemFileHeader)});
      info->header_b_info.length = file_b->GetFileSize();
    }
    {
      info->agg_info.file_number = mgr->NewFileNumber();
      auto agg_filename = root / EntityAggFileName(info->agg_info.file_number);
      std::unique_ptr<TsAppendOnlyFile> file_agg;
      ASSERT_EQ(env->NewAppendOnlyFile(agg_filename, &file_agg), SUCCESS);
      TsAggAndBlockFileHeader header_agg;
      header_agg.magic = TS_ENTITY_SEGMENT_AGG_FILE_MAGIC;
      header_agg.status = TsFileStatus::READY;
      file_agg->Append(TSSlice{(char *)&header_agg, sizeof(TsAggAndBlockFileHeader)});
      info->agg_info.length = file_agg->GetFileSize();
    }
    {
      info->datablock_info.file_number = mgr->NewFileNumber();
      auto data_filename = root / DataBlockFileName(info->datablock_info.file_number);
      std::unique_ptr<TsAppendOnlyFile> data_file;
      ASSERT_EQ(env->NewAppendOnlyFile(data_filename, &data_file), SUCCESS);
      TsAggAndBlockFileHeader header_block;
      header_block.magic = TS_ENTITY_SEGMENT_BLOCK_FILE_MAGIC;
      header_block.status = TsFileStatus::READY;
      data_file->Append(TSSlice{(char *)&header_block, sizeof(TsAggAndBlockFileHeader)});
      info->datablock_info.length = data_file->GetFileSize();
    }
  }

  void MimicAppendEntitySegment(TsVersionManager *mgr, const fs::path &root, TsEntitySegment *segment,
                                EntitySegmentMetaInfo *info) {
    auto prev_info = segment->GetHandleInfo();
    {
      auto header_b_filename = root / BlockHeaderFileName(prev_info.header_b_info.file_number);
      std::unique_ptr<TsAppendOnlyFile> file_b;
      ASSERT_EQ(env->NewAppendOnlyFile(header_b_filename, &file_b, false, prev_info.header_b_info.length), SUCCESS);
      std::string data(sizeof(TsEntitySegmentBlockItem), 0);
      file_b->Append(data);
      info->header_b_info.file_number = prev_info.header_b_info.file_number;
      info->header_b_info.length = file_b->GetFileSize();
    }
    {
      auto agg_filename = root / EntityAggFileName(prev_info.agg_info.file_number);
      std::unique_ptr<TsAppendOnlyFile> file_agg;
      ASSERT_EQ(env->NewAppendOnlyFile(agg_filename, &file_agg, false, prev_info.agg_info.length), SUCCESS);
      std::string data(17, 0);
      file_agg->Append(data);
      info->agg_info.file_number = prev_info.agg_info.file_number;
      info->agg_info.length = file_agg->GetFileSize();
    }
    {
      auto data_filename = root / DataBlockFileName(prev_info.datablock_info.file_number);
      std::unique_ptr<TsAppendOnlyFile> data_file;
      ASSERT_EQ(env->NewAppendOnlyFile(data_filename, &data_file, false, prev_info.datablock_info.length), SUCCESS);
      std::string data(97, 0);
      data_file->Append(data);
      info->datablock_info.file_number = prev_info.datablock_info.file_number;
      info->datablock_info.length = data_file->GetFileSize();
    }
  }

  void GenerateNewEntitySegment(TsVersionManager *mgr, PartitionIdentifier par_id, TsEntitySegment *segment,
                                EntitySegmentMetaInfo *info) {
    auto root = vgroup_root / PartitionDirName(par_id);
    {
      info->header_e_file_number = mgr->NewFileNumber();
      auto header_e_filename = root / EntityHeaderFileName(info->header_e_file_number);
      std::unique_ptr<TsAppendOnlyFile> file_e;
      ASSERT_EQ(env->NewAppendOnlyFile(header_e_filename, &file_e), SUCCESS);
      TsEntityItemFileHeader header_e;
      memset(&header_e, 0, sizeof(TsEntityItemFileHeader));
      header_e.magic = TS_ENTITY_SEGMENT_ENTITY_ITEM_FILE_MAGIC;
      header_e.encoding = 0;
      header_e.entity_num = 1;
      header_e.status = TsFileStatus::READY;

      TsEntityItem item;
      file_e->Append(TSSlice{(char *)&item, sizeof(TsEntityItem)});
      file_e->Append(TSSlice{(char *)&header_e, sizeof(TsEntityItemFileHeader)});
    }
    if (segment == nullptr) {
      BrandNewEntitySegment(mgr, root, info);
    } else {
      MimicAppendEntitySegment(mgr, root, segment, info);
    }
  }

  void MimicFlushing(TsVersionManager *mgr, const PartitionIdentifier par_id, bool force_kill = false) {
    TsVersionUpdate update;
    auto root = vgroup_root / PartitionDirName(par_id);
    uint64_t filenumber = mgr->NewFileNumber();
    auto last_seg_filename = root / LastSegmentFileName(filenumber);
    std::unique_ptr<TsAppendOnlyFile> file;
    ASSERT_EQ(env->NewAppendOnlyFile(last_seg_filename, &file), SUCCESS);
    TsLastSegmentBuilder builder(nullptr, std::move(file), filenumber);
    TsSegmentWriteStats stats;
    ASSERT_EQ(builder.Finalize(&stats), SUCCESS);
    update.AddLastSegment(par_id, {filenumber, 0, 0});
    if (!force_kill) {
      ASSERT_EQ(mgr->ApplyUpdate(&update), SUCCESS);
    }
  }

  void MimicCompaction(TsVersionManager *mgr, const PartitionIdentifier par_id, bool force_kill = false) {
    TsVersionUpdate update;
    auto root = vgroup_root / PartitionDirName(par_id);
    auto current = mgr->Current();
    auto partitions = current->GetPartitions(1, {all_data}, TIMESTAMP64);
    int level = -1, group = -1;
    auto lastsegments = partitions[0]->GetCompactLastSegments(&level, &group);
    uint64_t filenumber = mgr->NewFileNumber();
    auto last_seg_filename = root / LastSegmentFileName(filenumber);
    std::unique_ptr<TsAppendOnlyFile> file;
    ASSERT_EQ(env->NewAppendOnlyFile(last_seg_filename, &file), SUCCESS);
    TsLastSegmentBuilder builder(nullptr, std::move(file), filenumber);
    TsSegmentWriteStats stats;
    ASSERT_EQ(builder.Finalize(&stats), SUCCESS);
    update.AddLastSegment(par_id, {filenumber, std::min(level + 1, 3), 0});
    ASSERT_GE(lastsegments.size(), 2);
    update.DeleteLastSegment(par_id, lastsegments[0]->GetFileNumber());
    update.DeleteLastSegment(par_id, lastsegments[1]->GetFileNumber());

    auto p = mgr->Current()->GetPartition(par_id);
    ASSERT_NE(p, nullptr);
    auto entity_segment = p->GetEntitySegment();

    EntitySegmentMetaInfo info;
    GenerateNewEntitySegment(mgr, par_id, entity_segment.get(), &info);
    update.SetEntitySegment(par_id, info, false);

    if (!force_kill) {
      ASSERT_EQ(mgr->ApplyUpdate(&update), SUCCESS);
    }
  }

  void MimicVacuum(TsVersionManager *mgr, const PartitionIdentifier par_id, bool force_kill = false) {
    TsVersionUpdate update;
    auto root = vgroup_root / PartitionDirName(par_id);
    auto p = mgr->Current()->GetPartition(par_id);
    ASSERT_NE(p, nullptr);
    auto entity_segment = p->GetEntitySegment();

    EntitySegmentMetaInfo info;
    GenerateNewEntitySegment(mgr, par_id, nullptr, &info);
    update.SetEntitySegment(par_id, info, true);

    if (!force_kill) {
      ASSERT_EQ(mgr->ApplyUpdate(&update), SUCCESS);
    }
  }
};

TEST_F(TsVersionTest, EncodeDecodeTest) {
  {
    TsVersionUpdate update;
    update.AddMemSegment(nullptr);
    auto encoded = update.EncodeToString();
    EXPECT_EQ(encoded.size(), 0);
  }

  {
    TsVersionUpdate update;
    update.AddLastSegment({1, 2, 3}, {1, 0, 0});
    update.AddLastSegment({1, 2, 3}, {2, 1, 1});
    update.AddLastSegment({1, 2, 3}, {3, 2, 2});
    update.AddLastSegment({1, 2, 3}, {4, 3, 3});
    update.AddLastSegment({1, 2, 3}, {5, 3, 3});
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    auto decoded_str = decoded.EncodeToString();
    EXPECT_EQ(decoded_str.AsStringView(), encoded.AsStringView());
  }
  {
    TsVersionUpdate update;
    update.DeleteLastSegment({1, 2, 3}, 1);
    update.DeleteLastSegment({1, 2, 3}, 2);
    update.DeleteLastSegment({1, 2, 3}, 3);
    update.DeleteLastSegment({1, 2, 3}, 4);
    update.DeleteLastSegment({1, 2, 3}, 5);
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    auto decoded_str = decoded.EncodeToString();
    EXPECT_EQ(decoded_str.AsStringView(), encoded.AsStringView());
  }
  {
    TsVersionUpdate update;
    update.PartitionDirCreated({1, 2, 3});
    update.PartitionDirCreated({4, 5, 6});
    update.PartitionDirCreated({7, 8, 9});
    update.PartitionDirCreated({10, 11, 12});
    update.PartitionDirCreated({13, 14, 15});
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    auto decoded_str = decoded.EncodeToString();
    EXPECT_EQ(decoded_str.AsStringView(), encoded.AsStringView());
  }
  {
    TsVersionUpdate update;
    update.SetNextFileNumber(10000);
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    auto decoded_str = decoded.EncodeToString();
    EXPECT_EQ(decoded_str.AsStringView(), encoded.AsStringView());
  }

  // {
  //   TsVersionUpdate update;
  //   update.SetEntitySegment({1, 2, 3}, {1, 2, 3, 4});
  //   update.SetEntitySegment({4, 5, 6}, {4, 5, 6, 7});
  //   update.SetEntitySegment({7, 8, 9}, {7, 8, 9, 10});
  //   update.SetEntitySegment({10, 11, 12}, {10, 11, 12, 13});
  //   update.SetEntitySegment({13, 14, 15}, {13, 14, 15, 14});
  //   auto encoded = update.EncodeToString();
  //   EXPECT_NE(encoded.size(), 0);

  //   TsVersionUpdate decoded;
  //   TSSlice slice = {encoded.data(), encoded.size()};
  //   ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
  //   EXPECT_EQ(decoded.EncodeToString(), encoded);
  // }
  {
    TsVersionUpdate update;
    update.PartitionDirCreated({1, 2, 3});
    update.AddLastSegment({1, 2, 3}, {5, 0, 0});
    update.AddLastSegment({1, 2, 3}, {6, 1, 3});
    update.DeleteLastSegment({1, 2, 3}, 4);
    update.SetNextFileNumber(7);
    // update.SetEntitySegment({1, 2, 3}, {9, 10, 11, 12});
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    auto decoded_str = decoded.EncodeToString();
    EXPECT_EQ(decoded_str.AsStringView(), encoded.AsStringView());
  }
}

TEST_F(TsVersionTest, RecoverFromEmptyDirTest) {
  TsVersionManager mgr(env, vgroup_root);
  auto s = mgr.Recover();
  EXPECT_EQ(s, SUCCESS);
}

TEST_F(TsVersionTest, RecoverFromExistingDirTest) {
  std::vector<PartitionIdentifier> par_ids = {{{1, 2, 3}, {1, 5, 6}, {1, 8, 9}, {2, 11, 12}, {2, 14, 15}, {4, 14, 15}}};
  {
    std::unique_ptr<TsVersionManager> mgr;
    mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    auto s = mgr->Recover();
    EXPECT_EQ(s, SUCCESS);

    TsVersionUpdate update;
    for (auto pid : par_ids) {
      env->NewDirectory(vgroup_root / PartitionDirName(pid));
      update.PartitionDirCreated(pid);
    }
    s = mgr->ApplyUpdate(&update);
    EXPECT_EQ(s, SUCCESS);
  }

  {
    auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    auto s = mgr->Recover();
    EXPECT_EQ(s, SUCCESS);

    auto current = mgr->Current();
    auto partitions = current->GetPartitions(1, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions.size(), 3);
    partitions = current->GetPartitions(2, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions.size(), 2);
    partitions = current->GetPartitions(4, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions.size(), 1);

    {
      TsVersionUpdate update;
      env->NewDirectory(vgroup_root / PartitionDirName({1, 9, 10}));
      update.PartitionDirCreated({1, 9, 10});
      s = mgr->ApplyUpdate(&update);
      EXPECT_EQ(s, SUCCESS);
    }

    auto par_id = par_ids[0];
    auto all_partitions = mgr->Current()->GetAllPartitions();
    ASSERT_EQ(all_partitions.size(), par_ids.size() + 1);
    auto partition = all_partitions.begin()->second;
    // mimic flush
    for (int i = 0; i < 10; i++) {
      MimicFlushing(mgr.get(), par_id);
    }

    // mimic compaction
    {
      MimicCompaction(mgr.get(), par_id);
      MimicCompaction(mgr.get(), par_id);
    }
  }

  {
    auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    auto s = mgr->Recover();

    EXPECT_EQ(s, SUCCESS);

    auto current = mgr->Current();
    auto partitions = current->GetPartitions(1, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions.size(), 4);
    partitions = current->GetPartitions(2, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions.size(), 2);
    partitions = current->GetPartitions(4, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions.size(), 1);

    uint64_t expected_file_num = 10 /*flush*/ + 5 /*first compaction 4 (header.e header.b block agg) and 1 last*/ +
                                 2 /*second compaction 1 header.e and 1 last*/;
    EXPECT_EQ(mgr->NewFileNumber(), expected_file_num);

    partitions = current->GetPartitions(1, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions[0]->GetStartTime(), 2);
    EXPECT_EQ(partitions[0]->GetEndTime(), 3);
    auto lasts = partitions[0]->GetAllLastSegments();
    EXPECT_EQ(lasts.size(), 10 + 2 - 4);
  }
}

TEST_F(TsVersionTest, RecoverFromCorruptedDirTest) {
  PartitionIdentifier par_id = {1, 2, 3};
  auto partition_dir = vgroup_root / PartitionDirName(par_id);
  {
    env->NewDirectory(partition_dir);
    auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    auto s = mgr->Recover();
    EXPECT_EQ(s, SUCCESS);
    {
      TsVersionUpdate update;
      update.PartitionDirCreated(par_id);
      mgr->ApplyUpdate(&update);
    }

    // mimic flush
    for (int i = 0; i < 10; i++) {
      MimicFlushing(mgr.get(), par_id);
    }

    // mimic compaction
    {
      MimicCompaction(mgr.get(), par_id);
      MimicCompaction(mgr.get(), par_id);
    }

    for (int i = 0; i < 10; i++) {
      MimicFlushing(mgr.get(), par_id);
    }
  }

  // mimic crush when writing update to disk;
  {
    int fd = open((vgroup_root / "CURRENT").c_str(), O_RDONLY);
    ASSERT_GT(fd, 0);
    char buf[1024];
    int n = read(fd, buf, 1024);
    ASSERT_GE(n, 0);
    close(fd);
    ASSERT_LT(n, 1024);
    ASSERT_EQ(std::string_view(buf, n), "TSVERSION-000000000000\n");

    fd = open((vgroup_root / "TSVERSION-000000000000").c_str(), O_RDWR);
    ASSERT_GT(fd, 0);
    int size = lseek(fd, 0, SEEK_END);
    int ok = ftruncate(fd, size - 2);
    EXPECT_LE(ok, 0);
    close(fd);
  }

  {
    auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    auto s = mgr->Recover();
    ASSERT_EQ(s, SUCCESS);
    auto current = mgr->Current();
    auto partitions = current->GetPartitions(1, {all_data}, TIMESTAMP64);
    ASSERT_EQ(partitions.size(), 1);
    EXPECT_EQ(partitions[0]->GetAllLastSegments().size(),
              (2 * 10) /*flush*/ + (2 - 2 * 2) /*compaction*/ - 1 /*corruption*/);

    uint64_t expected_next = 10 + 5 + 2 + 10 - 1;
    EXPECT_EQ(mgr->NewFileNumber(), expected_next);

    auto path = partition_dir / LastSegmentFileName(expected_next);
    EXPECT_FALSE(fs::exists(path)) << path;
  }
}

TEST_F(TsVersionTest, RecoverAndDeletePartitionDir) {
  {
    std::vector<PartitionIdentifier> par_ids{
        {1, 2, 3},    {4, 5, 6},    {7, 8, 9},    {10, 11, 12}, {13, 14, 15},
        {16, 17, 18}, {19, 20, 21}, {22, 23, 24}, {25, 26, 27}, {28, 29, 30},
    };
    for (auto par_id : par_ids) {
      auto partition_dir = vgroup_root / PartitionDirName(par_id);
      ASSERT_EQ(env->NewDirectory(partition_dir), SUCCESS);
      ASSERT_TRUE(fs::exists(partition_dir));
    }
    auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    auto s = mgr->Recover();
    EXPECT_EQ(s, SUCCESS);
    {
      TsVersionUpdate update;
      for (int i = 0; i < 5; ++i) {
        update.PartitionDirCreated(par_ids[i]);
      }
      ASSERT_EQ(mgr->ApplyUpdate(&update), SUCCESS);
    }

    mgr.reset();

    mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    ASSERT_EQ(mgr->Recover(), SUCCESS);

    for (int i = 0; i < 10; ++i) {
      auto partition_dir = vgroup_root / PartitionDirName(par_ids[i]);
      if (i < 5) {
        ASSERT_TRUE(fs::exists(partition_dir));
      } else {
        ASSERT_FALSE(fs::exists(partition_dir));
      }
    }
  }
}

TEST_F(TsVersionTest, RecoverAndDelete_UnfinishedFlushing) {
  PartitionIdentifier par_id = {1, 2, 3};
  auto partition_dir = vgroup_root / PartitionDirName(par_id);
  env->NewDirectory(partition_dir);
  auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  auto s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);
  TsVersionUpdate update;
  update.PartitionDirCreated(par_id);
  mgr->ApplyUpdate(&update);
  for (int i = 0; i < 10; i++) {
    MimicFlushing(mgr.get(), par_id);
  }
  MimicFlushing(mgr.get(), par_id, true);

  mgr.reset();
  mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);

  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(i)));
  }
  EXPECT_FALSE(fs::exists(partition_dir / LastSegmentFileName(10)));
}

TEST_F(TsVersionTest, RecoverAndDelete_UnfinishedCompaction) {
  PartitionIdentifier par_id = {1, 2, 3};
  auto partition_dir = vgroup_root / PartitionDirName(par_id);
  env->NewDirectory(partition_dir);
  auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  auto s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);
  TsVersionUpdate update;
  update.PartitionDirCreated(par_id);
  mgr->ApplyUpdate(&update);
  for (int i = 0; i < 10; i++) {
    MimicFlushing(mgr.get(), par_id);
  }
  // first compaction
  // Add: last-10 L1, header.e-11, header.b-12, agg-13, data-14
  // Del: last-0,  last-1
  MimicCompaction(mgr.get(), par_id, false);
  // second compaction
  // Add: last-15 L1, header.e-16
  // Del: last-2,  last-3
  MimicCompaction(mgr.get(), par_id, false);
  // third compaction compact L1
  // Add: last-17 L2, header.e-18
  // Del: last-10,  last-15
  MimicCompaction(mgr.get(), par_id, false);
  // unfinished compaction
  // Add: last-19 L1, header.e-20
  // Del: last-4,  last-5
  MimicCompaction(mgr.get(), par_id, true);

  mgr.reset();
  mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);

  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(4)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(5)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(6)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(7)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(8)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(9)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(17)));

  EXPECT_FALSE(fs::exists(partition_dir / LastSegmentFileName(10)));
  EXPECT_FALSE(fs::exists(partition_dir / LastSegmentFileName(15)));
  EXPECT_FALSE(fs::exists(partition_dir / LastSegmentFileName(19)));

  EXPECT_TRUE(fs::exists(partition_dir / EntityAggFileName(13)));
  EXPECT_TRUE(fs::exists(partition_dir / EntityHeaderFileName(18)));
  EXPECT_TRUE(fs::exists(partition_dir / BlockHeaderFileName(12)));
  EXPECT_TRUE(fs::exists(partition_dir / DataBlockFileName(14)));

  EXPECT_FALSE(fs::exists(partition_dir / EntityHeaderFileName(20)));
  EXPECT_FALSE(fs::exists(partition_dir / EntityHeaderFileName(11)));
  EXPECT_FALSE(fs::exists(partition_dir / EntityHeaderFileName(16)));

  EXPECT_EQ(fs::file_size(partition_dir / DataBlockFileName(14)), sizeof(TsAggAndBlockFileHeader) + 97 * 2);
  EXPECT_EQ(fs::file_size(partition_dir / EntityAggFileName(13)), sizeof(TsAggAndBlockFileHeader) + 17 * 2);
  EXPECT_EQ(fs::file_size(partition_dir / BlockHeaderFileName(12)),
            sizeof(TsBlockItemFileHeader) + sizeof(TsEntitySegmentBlockItem) * 2);
}


TEST_F(TsVersionTest, RecoverAndDelete_UnfinishedVacuum) {
  PartitionIdentifier par_id = {1, 2, 3};
  auto partition_dir = vgroup_root / PartitionDirName(par_id);
  env->NewDirectory(partition_dir);
  auto mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  auto s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);
  TsVersionUpdate update;
  update.PartitionDirCreated(par_id);
  mgr->ApplyUpdate(&update);
  for (int i = 0; i < 10; i++) {
    MimicFlushing(mgr.get(), par_id);
  }
  // first compaction
  // Add: last-10 L1, header.e-11, header.b-12, agg-13, data-14
  // Del: last-0,  last-1
  MimicCompaction(mgr.get(), par_id, false);
  // second compaction
  // Add: last-15 L1, header.e-16
  // Del: last-2,  last-3
  MimicCompaction(mgr.get(), par_id, false);
  // third compaction compact L1
  // Add: last-17, header.e-18
  // Del: last-10,  last-15
  MimicCompaction(mgr.get(), par_id, false);

  // unfinished vacuum
  // Add: header.e-19, header.b-20, agg-21, data-22
  // Del: header.e-11, header.b-12, agg-13, data-14
  MimicVacuum(mgr.get(), par_id, true);

  EXPECT_TRUE(fs::exists(partition_dir / EntityAggFileName(21)));
  EXPECT_TRUE(fs::exists(partition_dir / EntityHeaderFileName(19)));
  EXPECT_TRUE(fs::exists(partition_dir / BlockHeaderFileName(20)));
  EXPECT_TRUE(fs::exists(partition_dir / DataBlockFileName(22)));

  mgr.reset();
  mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);

  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(4)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(5)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(6)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(7)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(8)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(9)));
  EXPECT_FALSE(fs::exists(partition_dir / LastSegmentFileName(10)));
  EXPECT_FALSE(fs::exists(partition_dir / LastSegmentFileName(15)));
  EXPECT_TRUE(fs::exists(partition_dir / LastSegmentFileName(17)));

  EXPECT_TRUE(fs::exists(partition_dir / EntityAggFileName(13)));
  EXPECT_TRUE(fs::exists(partition_dir / EntityHeaderFileName(18)));
  EXPECT_TRUE(fs::exists(partition_dir / BlockHeaderFileName(12)));
  EXPECT_TRUE(fs::exists(partition_dir / DataBlockFileName(14)));

  EXPECT_EQ(fs::file_size(partition_dir / DataBlockFileName(14)), sizeof(TsAggAndBlockFileHeader) + 97 * 2);
  EXPECT_EQ(fs::file_size(partition_dir / EntityAggFileName(13)), sizeof(TsAggAndBlockFileHeader) + 17 * 2);
  EXPECT_EQ(fs::file_size(partition_dir / BlockHeaderFileName(12)),
            sizeof(TsBlockItemFileHeader) + sizeof(TsEntitySegmentBlockItem) * 2);

  EXPECT_FALSE(fs::exists(partition_dir / EntityAggFileName(21)));
  EXPECT_FALSE(fs::exists(partition_dir / EntityHeaderFileName(19)));
  EXPECT_FALSE(fs::exists(partition_dir / BlockHeaderFileName(20)));
  EXPECT_FALSE(fs::exists(partition_dir / DataBlockFileName(22)));

  // recover again and do vacuum again
  mgr.reset();
  mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);

  // unfinished vacuum
  // Add: header.e-19, header.b-20, agg-21, data-22
  // Del: header.e-11, header.b-12, agg-13, data-14
  MimicVacuum(mgr.get(), par_id, false);

  EXPECT_TRUE(fs::exists(partition_dir / EntityAggFileName(21)));
  EXPECT_TRUE(fs::exists(partition_dir / EntityHeaderFileName(19)));
  EXPECT_TRUE(fs::exists(partition_dir / BlockHeaderFileName(20)));
  EXPECT_TRUE(fs::exists(partition_dir / DataBlockFileName(22)));

  EXPECT_FALSE(fs::exists(partition_dir / EntityAggFileName(13)));
  EXPECT_FALSE(fs::exists(partition_dir / EntityHeaderFileName(18)));
  EXPECT_FALSE(fs::exists(partition_dir / BlockHeaderFileName(12)));
  EXPECT_FALSE(fs::exists(partition_dir / DataBlockFileName(14)));

  mgr.reset();
  mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
  s = mgr->Recover();
  EXPECT_EQ(s, SUCCESS);
  EXPECT_TRUE(fs::exists(partition_dir / EntityAggFileName(21)));
  EXPECT_TRUE(fs::exists(partition_dir / EntityHeaderFileName(19)));
  EXPECT_TRUE(fs::exists(partition_dir / BlockHeaderFileName(20)));
  EXPECT_TRUE(fs::exists(partition_dir / DataBlockFileName(22)));
}
