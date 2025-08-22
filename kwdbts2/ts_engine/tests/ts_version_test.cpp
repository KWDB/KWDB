#include "ts_version.h"

#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <string_view>

#include "data_type.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_entity_segment_handle.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_lastsegment_builder.h"

class TsVersionTest : public testing::Test {
 protected:
  TsIOEnv *env = &TsMMapIOEnv::GetInstance();
  fs::path vgroup_root = "version_test";

  KwTsSpan all_data{INT64_MIN, INT64_MAX};

  void SetUp() override {
    env->DeleteDir(vgroup_root);
    env->NewDirectory(vgroup_root);
  }

  void MimicFlushing(TsVersionManager *mgr, PartitionIdentifier par_id) {
    TsVersionUpdate update;
    auto root = vgroup_root / PartitionDirName(par_id);
    uint64_t filenumber = mgr->NewFileNumber();
    auto last_seg_filename = root / LastSegmentFileName(filenumber);
    std::unique_ptr<TsAppendOnlyFile> file;
    ASSERT_EQ(env->NewAppendOnlyFile(last_seg_filename, &file), SUCCESS);
    TsLastSegmentBuilder builder(nullptr, std::move(file), filenumber);
    ASSERT_EQ(builder.Finalize(), SUCCESS);
    update.AddLastSegment(par_id, filenumber);
    ASSERT_EQ(mgr->ApplyUpdate(&update), SUCCESS);
  }

  void MimicCompaction(TsVersionManager *mgr, PartitionIdentifier par_id, EntitySegmentHandleInfo info) {
    TsVersionUpdate update;
    auto root = vgroup_root / PartitionDirName(par_id);
    auto current = mgr->Current();
    auto partitions = current->GetPartitions(1, {all_data}, TIMESTAMP64);
    auto lastsegments = partitions[0]->GetAllLastSegments();
    uint64_t filenumber = mgr->NewFileNumber();
    auto last_seg_filename = root / LastSegmentFileName(filenumber);
    std::unique_ptr<TsAppendOnlyFile> file;
    ASSERT_EQ(env->NewAppendOnlyFile(last_seg_filename, &file), SUCCESS);
    TsLastSegmentBuilder builder(nullptr, std::move(file), filenumber);
    ASSERT_EQ(builder.Finalize(), SUCCESS);
    update.AddLastSegment(par_id, filenumber);
    ASSERT_GE(lastsegments.size(), 2);
    update.DeleteLastSegment(par_id, lastsegments[0]->GetFileNumber());
    update.DeleteLastSegment(par_id, lastsegments[1]->GetFileNumber());
    // update.SetEntitySegment(par_id, info);
    ASSERT_EQ(mgr->ApplyUpdate(&update), SUCCESS);
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
    update.AddLastSegment({1, 2, 3}, 1);
    update.AddLastSegment({1, 2, 3}, 2);
    update.AddLastSegment({1, 2, 3}, 3);
    update.AddLastSegment({1, 2, 3}, 4);
    update.AddLastSegment({1, 2, 3}, 5);
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    EXPECT_EQ(decoded.EncodeToString(), encoded);
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
    EXPECT_EQ(decoded.EncodeToString(), encoded);
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
    EXPECT_EQ(decoded.EncodeToString(), encoded);
  }
  {
    TsVersionUpdate update;
    update.SetNextFileNumber(10000);
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    EXPECT_EQ(decoded.EncodeToString(), encoded);
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
    update.AddLastSegment({1, 2, 3}, 5);
    update.AddLastSegment({1, 2, 3}, 6);
    update.DeleteLastSegment({1, 2, 3}, 4);
    update.SetNextFileNumber(7);
    // update.SetEntitySegment({1, 2, 3}, {9, 10, 11, 12});
    auto encoded = update.EncodeToString();
    EXPECT_NE(encoded.size(), 0);

    TsVersionUpdate decoded;
    TSSlice slice = {encoded.data(), encoded.size()};
    ASSERT_EQ(decoded.DecodeFromSlice(slice), SUCCESS);
    EXPECT_EQ(decoded.EncodeToString(), encoded);
  }
}

TEST_F(TsVersionTest, RecoverFromEmptyDirTest) {
  TsVersionManager mgr(env, vgroup_root);
  auto s = mgr.Recover();
  EXPECT_EQ(s, SUCCESS);
}

TEST_F(TsVersionTest, RecoverFromExistingDirTest) {
  PartitionIdentifier par_id = {1, 2, 3};
  auto partition_dir = vgroup_root / PartitionDirName(par_id);
  env->NewDirectory(partition_dir);

  {
    std::unique_ptr<TsVersionManager> mgr;
    mgr = std::make_unique<TsVersionManager>(env, vgroup_root);
    auto s = mgr->Recover();
    EXPECT_EQ(s, SUCCESS);

    TsVersionUpdate update;
    std::vector<PartitionIdentifier> par_ids = {
        {{1, 2, 3}, {1, 5, 6}, {1, 8, 9}, {2, 11, 12}, {2, 14, 15}, {4, 14, 15}}};
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

    // mimic flush
    for (int i = 0; i < 10; i++) {
      MimicFlushing(mgr.get(), par_id);
    }

    // mimic compaction
    {
      MimicCompaction(mgr.get(), par_id, {{1, 2}, {3, 4}, {5, 6}, 7});
      MimicCompaction(mgr.get(), par_id, {{7, 6}, {5, 4}, {3, 2}, 1});
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
    EXPECT_EQ(mgr->NewFileNumber(), 12);

    partitions = current->GetPartitions(1, {all_data}, TIMESTAMP64);
    EXPECT_EQ(partitions[0]->GetStartTime(), 2);
    EXPECT_EQ(partitions[0]->GetEndTime(), 3);
    auto lasts = partitions[0]->GetAllLastSegments();
    EXPECT_EQ(lasts.size(), 10 + 2 - 4);
  }
}

TEST_F(TsVersionTest, RecoverFromCorruptedDirTest) {
  {
    PartitionIdentifier par_id = {1, 2, 3};
    auto partition_dir = vgroup_root / PartitionDirName(par_id);
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
      MimicCompaction(mgr.get(), par_id, {{1,2},{3,4},{5,6},7});
      MimicCompaction(mgr.get(), par_id, {{7,6},{5,4},{3,2},1});
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
    EXPECT_EQ(mgr->NewFileNumber(), 2 * 10 + 2 - 1);
  }
}
