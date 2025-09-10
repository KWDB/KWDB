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

// #include <gtest/gtest.h>
// #include "ts_sampler.h"

// namespace kwdbts {

// void CreateSketches(std::shared_ptr<TSSamplerSpec> *sampler) {
//     for (k_uint32 i = 0; i < 7; ++i) {
//         kwdbts::SketchInfo* newSketch = (*sampler)->add_sketches();
//         newSketch->set_sketch_type(static_cast<SketchMethod>(SketchMethod_HLL_PLUS_PLUS));

//         newSketch->add_col_id(i);
//         newSketch->add_col_type(NormalCol);
//         newSketch->set_generatehistogram(false);
//     }
//     kwdbts::SketchInfo* newSketch = (*sampler)->add_sketches();
//     newSketch->set_sketch_type(static_cast<SketchMethod>(SketchMethod_HLL_PLUS_PLUS));
//     newSketch->add_col_id(7);
//     newSketch->add_col_type(PrimaryTag);
//     newSketch->set_generatehistogram(true);
//     newSketch->set_hasallptag(true);
// }

// void CreateReaderSpec(std::shared_ptr<TSReaderSpec> *reader) {
//     reader->reset(KNEW TSReaderSpec());
//     // (*reader)->set_accessmode(TSTableReadMode::tableTableMeta);
// }

// void CreateTagReaderSpec(std::shared_ptr<TSTagReaderSpec> *tagReaderSpec) {
//     tagReaderSpec->reset(KNEW TSTagReaderSpec());
//     for (k_int32 i = 0; i < 8; ++i) {
//         (*tagReaderSpec)->add_colmetas();
//     }
//     (*tagReaderSpec)->set_tableid(80);
//     (*tagReaderSpec)->set_accessmode(TSTableReadMode::metaTable);
// }

// void CreateSamplerSpec(std::shared_ptr<TSSamplerSpec> *sampler) {
//     sampler->reset(KNEW TSSamplerSpec());
//     (*sampler)->set_sample_size(10000);
//     (*sampler)->set_table_id(80);
//     CreateSketches(sampler);
// }

// void CreateKWDBPostProcessSpec(std::shared_ptr<PostProcessSpec>* post) {
//     post->reset(KNEW PostProcessSpec());
//     PostProcessSpec *spec = post->get();
//     for (k_int32 i = 0; i < 8; ++i) {
//         // spec->add_tscols();
//         spec->add_outputcols(i);
//     }
//     spec->add_outputtypes(KWDBTypeFamily::TimestampFamily);
//     spec->add_outputtypes(KWDBTypeFamily::IntFamily);
//     spec->add_outputtypes(KWDBTypeFamily::IntFamily);
//     spec->add_outputtypes(KWDBTypeFamily::FloatFamily);
//     spec->add_outputtypes(KWDBTypeFamily::FloatFamily);
//     spec->add_outputtypes(KWDBTypeFamily::StringFamily);
//     spec->add_outputtypes(KWDBTypeFamily::StringFamily);
//     spec->add_outputtypes(KWDBTypeFamily::IntFamily);
// }

// class TestSampler : public ::testing::Test {
//  protected:
//     KDatabaseId schemaID_;
//     KTableId tableID_;
//     k_uint32 output_size_;
//     k_uint32 sample_size_;
//     kwdbContext_t context_;
//     kwdbContext_p ctx_;
//     std::shared_ptr<TABLE> table_;
//     std::shared_ptr<TSTagReaderSpec> tagReaderSpec_;
//     std::shared_ptr<TSReaderSpec> readerSpec_;
//     std::shared_ptr<PostProcessSpec> post_;
//     std::shared_ptr<TSSamplerSpec> samplerSpec_;
//     std::unique_ptr<BaseOperator> input_;
//     std::shared_ptr<TagScanIterator> tagScanIterator_;
//     std::shared_ptr<PostProcessSpec> tagpost_;

//     static void SetUpTestCase() {}

//     static void TearDownTestCase() {}

//     void SetUp() override {
//         ctx_ = &context_;
//         InitServerKWDBContext(ctx_);
//         schemaID_ = 79;
//         tableID_ = 80;
//         output_size_ = 8;
//         sample_size_ = 10000;
//         table_.reset(KNEW TABLE(schemaID_, tableID_));
//         CreateTagReaderSpec(&tagReaderSpec_);
//         CreateReaderSpec(&readerSpec_);
//         CreateKWDBPostProcessSpec(&post_);
//         CreateSamplerSpec(&samplerSpec_);
//         table_->Init(ctx_, tagReaderSpec_.get());
//         CreateKWDBPostProcessSpec(&tagpost_);

//         tagScanIterator_ = std::make_shared<TagScanIterator>(tagReaderSpec_.get(), tagpost_.get(), table_.get(), 0);
//         input_ = make_unique<TableScanIterator>(readerSpec_.get(), post_.get(), table_.get(), tagScanIterator_.get(), 0);
//     }

//     void TearDown() override {}
// };

// TEST_F(TestSampler, SamplerSetup) {
//     EXPECT_EQ(table_->FieldCount(), output_size_);
//     EXPECT_EQ((*samplerSpec_).sketches_size(), output_size_);
//     EXPECT_EQ((*samplerSpec_).table_id(), tableID_);
//     EXPECT_EQ((*samplerSpec_).sample_size(), sample_size_);
//     std::shared_ptr<TsSampler> sampler = std::make_shared<TsSampler>(table_.get(), input_.get(), 0);
//     ASSERT_TRUE(sampler->setup(samplerSpec_.get()) == SUCCESS);
//     EXPECT_EQ(sampler->GetSampleSize(), sample_size_);
//     EXPECT_EQ(sampler->GetSketches<NormalCol>().size(), output_size_ - 1);
//     EXPECT_EQ(sampler->GetSketches<Tag>().size(), 0);
//     EXPECT_EQ(sampler->GetSketches<PrimaryTag>().size(), 1);
// }

// TEST_F(TestSampler, SamplerPreInit) {
//     EXPECT_EQ(table_->FieldCount(), output_size_);
//     std::shared_ptr<TsSampler> sampler = std::make_shared<TsSampler>(table_.get(), input_.get(), 0);
//     EXPECT_EQ(input_->GetRenderSize(), 0);
//     ASSERT_TRUE(sampler->setup(samplerSpec_.get()) == SUCCESS);
//     if (sampler->Init(ctx_) == EE_OK) {
//       EXPECT_EQ(input_->GetRenderSize(), output_size_);
//       EXPECT_EQ(sampler->Close(ctx_), SUCCESS);
//     }
// }

// TEST_F(TestSampler, SamplerNext) {
//     std::shared_ptr<TsSampler> sampler = std::make_shared<TsSampler>(table_.get(), input_.get(), 0);
//     ASSERT_TRUE(sampler->setup(samplerSpec_.get()) == SUCCESS);
//     ASSERT_TRUE(sampler->Init(ctx_) == EE_OK);
// //    ASSERT_TRUE(sampler->Next(ctx_) == EE_ERROR);
// }

// TEST_F(TestSampler, SamplerMainLoop) {
//     std::shared_ptr<TsSampler> sampler = std::make_shared<TsSampler>(table_.get(), input_.get(), 0);
//     ASSERT_TRUE(sampler->setup(samplerSpec_.get()) == SUCCESS);
//     ASSERT_TRUE(sampler->Init(ctx_) == EE_OK);
//     EXPECT_EQ(sampler->GetSketches<NormalCol>().size(), output_size_ - 1);
// //    ASSERT_TRUE(sampler->mainLoop<NormalCol>(ctx_) == EE_ERROR);
//     EXPECT_EQ(sampler->GetSketches<Tag>().size(), 0);
//     EXPECT_EQ(sampler->GetSketches<PrimaryTag>().size(), 1);
// //    ASSERT_TRUE(sampler->mainLoop<PrimaryTag>(ctx_) == EE_ERROR);
// }

// TEST_F(TestSampler, TestAssignDataToRow) {
//     SampledRow row{};

//     // TIMESTAMP
//     const k_int64 timestampData = 1704869668000;
//     AssignTagDataToRow(&row, const_cast<k_int64*>(&timestampData), false, KWDBTypeFamily::IntFamily, sizeof(k_int64));
//     EXPECT_TRUE(row.data.has_value());
//     EXPECT_EQ(std::get<k_int64>(row.data.value()), 1704869668000);

//     // INT
//     const k_int64 intData = 16;
//     AssignTagDataToRow(&row, const_cast<k_int64*>(&intData), false, KWDBTypeFamily::IntFamily, sizeof(k_int64));
//     EXPECT_TRUE(row.data.has_value());
//     EXPECT_EQ(std::get<k_int64>(row.data.value()), 16);

//     // DOUBLE
//     const k_double64 floatData = 3.14f;
//     AssignTagDataToRow(&row, const_cast<k_double64*>(&floatData), false, KWDBTypeFamily::FloatFamily, sizeof(k_double64));
//     EXPECT_TRUE(row.data.has_value());
//     EXPECT_NEAR(std::get<k_double64>(row.data.value()), 3.14, 1e-6);

//     // STRING
//     const char* strData = "test";
//     AssignTagDataToRow(&row, const_cast<char*>(strData), false, KWDBTypeFamily::StringFamily, strlen(strData));
//     EXPECT_TRUE(row.data.has_value());
//     EXPECT_EQ(std::get<std::string>(row.data.value()), "test");

//     // NULL
//     AssignTagDataToRow(&row, nullptr, true, KWDBTypeFamily::IntFamily, sizeof(int));
//     EXPECT_FALSE(row.data.has_value());
// }
//
//}  // namespace kwdbts

