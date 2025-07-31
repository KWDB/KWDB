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

#include <memory>
#include <vector>
#include <optional>

#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"
#include "ee_base_op.h"
#include "row_sampling.h"
#include "ts_hyperloglog.h"
#include "ee_data_chunk.h"
#include "ee_kwthd_context.h"
#include "engine.h"

enum SketchMethods {
    // Computes the count-distinct statistical algorithm for columns
    SketchMethod_HLL_PLUS_PLUS  = 0
};

enum sampleObject {
    Metrics,
    Tag,
    PrimaryTag,
    SortedHistogram
};

// minBucketSpan is minimum time span for each bucket
const int minBucketSpan = 3600000;

namespace kwdbts {

// Estimate sketch for each column
struct SketchSpec {
    // Algorithm for calculating count-distinct
    k_uint32 sketch_type{0};
    // Estimate sketch
    std::shared_ptr<Sketch> sketch;
    // Whether you use histogram
    k_bool histogram{false};
    // Reservoir
    std::shared_ptr<SampleReservoir> reservoir;
    // Column of statistic, currently only supports single column
    // TODO(zh): Multiple columns
    vector<k_uint32> statsCol_idx{0};
    // Primary tag, tag, or normal column
    vector<k_uint32> statsCol_typ{0};
    k_uint32 numNulls{0};
    k_uint32 numRows{0};
    // Currently sketch index
    k_uint32 sketchIdx{0};
    roachpb::KWDBKTSColumn::ColumnType column_type;

    // addRow adds a row to the sketch and updates row counts.
    void addRow(BaseOperator* input, RowBatch* data_handle);
};

// Sort Histogram info
struct SortHistogram {
  kwdbts::SortedHistogramInfo histogram_info;

  // Store sorted histogram data
  vector<vector<optional<DataVariant>>> histogram_data;

  // Currently sketch index
  k_uint32 sketchIdx{0};
};

class TsSamplerOperator : public BaseOperator {
 public:
  explicit TsSamplerOperator(TsFetcherCollection* collection, TABLE* table, int32_t processor_id);

  ~TsSamplerOperator() override = default;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_SAMPLER;}

  /*
   * @Description : Sets up the TsSamplerOperator with sampling specifications from a given TSSamplerSpec.
   *                Initializes sketches for normal columns, primary tag columns, and tag columns based on the spec.
   * @IN tsInfo : Pointer to a TSSamplerSpec structure containing the specifications for how sampling should be performed.
   * @OUT normalCol_sketches_ : Vector of SketchSpec structures for normal columns initialized based on tsInfo.
   * @OUT primary_tag_sketches_ : Vector of SketchSpec structures for primary tag columns initialized based on tsInfo.
   * @OUT tag_sketches_ : Vector of SketchSpec structures for tag columns initialized based on tsInfo.
   * @OUT sample_size_ : The size of samples to collect, derived from tsInfo.
   * @Return KStatus : Status code indicating SUCCESS or FAIL of the setup process.
   */
  KStatus setup(const TSSamplerSpec* tsInfo);

  /*
   * @Description : Main processing loop for sampling and collecting statistics on different column types in time series tables.
   *                The function's behavior is specialized based on the template parameter indicating the type of columns to process
   *                (NormalCol, Tag, or PrimaryTag).
   * @IN ctx : Context of the current database kernel, contains session information.
   * @IN input_ : Pointer to the input source, typically a table scan operator that provides rows for sampling.
   * @IN current_thd : Indicates the current thread's context, used to check for proper thread management.
   * @OUT normalCol_sketches_ : Vector holding sketch structures for normal columns, updated with sampled data and statistics.
   * @OUT primary_tag_sketches_ : Vector holding sketch structures for primary tag columns, updated similarly.
   * @Return EEIteratorErrCode : Error code representing the status of the function execution.
   */
  template<sampleObject N>
  EEIteratorErrCode mainLoop(kwdbContext_p ctx);

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  k_uint32 GetSampleSize() const;

  template<sampleObject N>
  std::vector<SketchSpec> GetSketches() const;

  EEIteratorErrCode Reset(kwdbContext_p ctx);

  void AddData(const vector<optional<DataVariant>>& row_data, DataChunkPtr& chunk);

  KStatus GetSampleResult(kwdbContext_p ctx, DataChunkPtr& chunk);

  KStatus ProcessSketches(kwdbContext_p ctx, const std::vector<SketchSpec>& sketches, DataChunkPtr& chunk);

 private:
  // All sketch of statistic columns of normal columns
  std::vector<SketchSpec> normalCol_sketches_;

  // All sketch of statistic columns of primary tag columns
  std::vector<SketchSpec> primary_tag_sketches_;

  // All sketch of statistic columns of tag columns
  std::vector<SketchSpec> tag_sketches_;

  std::shared_ptr<TsTable> ts_table_{nullptr};

  // Output return types
  std::vector<KWDBTypeFamily> outRetrunTypes_;

  // Output store type
  std::vector<roachpb::DataType> outStorageTypes_;

  // Output store length
  std::vector<k_int32> outLens_;

  uint32_t sample_size_{};
  /* Output columns
   rankCol_: sampled data's rank
   sketchIdxCol_: index of statistic column
   numRowsCol_: index of total row count
   numNullsCol_: index of total null count
   sketchCol_: index of sketch col, used for calculation distinct-count
  */
  k_uint32 rankCol_{0};
  k_uint32 sketchIdxCol_{0};
  k_uint32 numRowsCol_{0};
  k_uint32 numNullsCol_{0};
  k_uint32 sketchCol_{0};
  k_uint32 bucketIDCol_{0};
  k_uint32 bucketNumRowsCol_{0};

  SortHistogram sorted_histogram_{};
  k_bool is_done_{false};
  k_uint32 total_sample_rows_{0};
};

/*
 * @Description : Assigns data to a given row structure based on the type of the data and whether it is null.
 *                This function reads the value from a given field and assigns it to the row's data member.
 * @IN row : Pointer to a SampledRow structure where the data will be stored.
 * @IN render : Pointer to a Field that contains the data to be assigned to the row.
 * @IN is_null : Boolean flag indicating whether the data in the field is null.
 * @IN type : Enum value representing the family type of the data in the field, used to determine how to correctly interpret the data.
 * @OUT row->data : The data member of the row is updated with the value fetched from the field, if the data is not null.
 */
void AssignDataToRow(SampledRow* row, Field *render, bool is_null, KWDBTypeFamily type);

// EncodeBytes appends the encoded datum to the given slice using the requested
// encoding.
std::vector<byte> EncodeBytes(Field* render, bool isNull, std::vector<byte>& appendTo);

}  // namespace kwdbts
