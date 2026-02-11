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

#include "ee_aggregate_func.h"

namespace kwdbts {

int AggregateFunc::isDistinct(IChunk* chunk, k_uint32 line,
                                 std::vector<roachpb::DataType>& col_types,
                                 std::vector<k_uint32>& col_lens,
                                 std::vector<k_uint32>& group_cols,
                                 k_bool *is_distinct,
                                 std::vector<bool>& group_allow_null) {
  // group col + agg col
  std::vector<k_uint32> all_cols(group_cols);
  all_cols.insert(all_cols.end(), arg_idx_.begin(), arg_idx_.end());

  if (seen_ == nullptr) {
    std::vector<roachpb::DataType> distinct_types;
    std::vector<k_uint32> distinct_lens;
    std::vector<bool> distinct_allow_null;
    for (auto& col : all_cols) {
      distinct_types.push_back(col_types[col]);
      distinct_lens.push_back(col_lens[col]);
      distinct_allow_null.push_back(group_allow_null[col]);
    }
    seen_ = KNEW LinearProbingHashTable(distinct_types, distinct_lens, 0, distinct_allow_null);
    if (seen_->Resize() < 0) {
      return -1;
    }
  }

  k_uint64 loc;
  if (seen_->FindOrCreateGroups(chunk, line, all_cols, &loc) < 0) {
    return -1;
  }

  if (seen_->IsUsed(loc)) {
    *is_distinct = false;
    return 0;
  }

  seen_->SetUsed(loc);
  seen_->CopyGroups(chunk, line, all_cols, loc);

  *is_distinct = true;
  return 0;
}

AggregateFunc* AggregateFuncFactory::CreateMax(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::BOOL:
      agg_func = KNEW MaxAggregate<k_bool>(col_index, col_id, len);
      break;
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW MaxAggregate<k_int16>(col_index, col_id, len);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW MaxAggregate<k_int32>(col_index, col_id, len);
      break;
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT:
      agg_func = KNEW MaxAggregate<k_int64>(col_index, col_id, len);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW MaxAggregate<k_float32>(col_index, col_id, len);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW MaxAggregate<k_double64>(col_index, col_id, len);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY:
      agg_func = KNEW MaxAggregate<String>(col_index, col_id, len + STRING_WIDE);
      break;
    case roachpb::DataType::DECIMAL:
      agg_func = KNEW MaxAggregate<k_decimal>(col_index, col_id, len + BOOL_WIDE);
      break;
    default:
      LOG_ERROR("unsupported data type for max aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for max aggregation");
      break;
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateMin(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::BOOL:
      agg_func = KNEW MinAggregate<k_bool>(col_index, col_id, len);
      break;
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW MinAggregate<k_int16>(col_index, col_id, len);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW MinAggregate<k_int32>(col_index, col_id, len);
      break;
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT:
      agg_func = KNEW MinAggregate<k_int64>(col_index, col_id, len);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW MinAggregate<k_float32>(col_index, col_id, len);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW MinAggregate<k_double64>(col_index, col_id, len);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY:
      agg_func = KNEW MinAggregate<String>(col_index, col_id, len + STRING_WIDE);
      break;
    case roachpb::DataType::DECIMAL:
      agg_func = KNEW MinAggregate<k_decimal>(col_index, col_id, len + BOOL_WIDE);
      break;
    default:
      LOG_ERROR("unsupported data type for max aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for max aggregation");
      break;
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateAnyNotNull(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::BOOL:
      agg_func = KNEW AnyNotNullAggregate<k_bool>(col_index, col_id, len);
      break;
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW AnyNotNullAggregate<k_int16>(col_index, col_id, len);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW AnyNotNullAggregate<k_int32>(col_index, col_id, len);
      break;
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT:
      agg_func = KNEW AnyNotNullAggregate<k_int64>(col_index, col_id, len);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW AnyNotNullAggregate<k_float32>(col_index, col_id, len);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW AnyNotNullAggregate<k_double64>(col_index, col_id, len);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY:
      agg_func = KNEW AnyNotNullAggregate<std::string>(col_index, col_id, len + STRING_WIDE);
      break;
    case roachpb::DataType::DECIMAL:
      agg_func = KNEW AnyNotNullAggregate<k_decimal>(col_index, col_id, len + BOOL_WIDE);
      break;
    default:
      LOG_ERROR("unsupported data type for any_not_null aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for not null aggregation");
      break;
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateSum(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW SumAggregate<k_int16, k_decimal>(col_index, col_id, len);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW SumAggregate<k_int32, k_decimal>(col_index, col_id, len);
      break;
    case roachpb::DataType::BIGINT:
      agg_func = KNEW SumAggregate<k_int64, k_decimal>(col_index, col_id, len);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW SumAggregate<k_float32, k_double64>(col_index, col_id, len);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW SumAggregate<k_double64, k_double64>(col_index, col_id, len);
      break;
    case roachpb::DataType::DECIMAL:
      agg_func = KNEW SumAggregate<k_decimal, k_decimal>(col_index, col_id, len + BOOL_WIDE);
      break;
    default:
      LOG_ERROR("unsupported data type for sum aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for sum aggregation");
      break;
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateSumInt(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW SumIntAggregate(col_index, col_id, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateCount(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW CountAggregate(col_index, col_id, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateCountRow(k_int32 col_index, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW CountRowAggregate(col_index, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateLast(roachpb::DataType storage_type,
                                                                      k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                                      k_uint32 ts_col_id, k_int64 time) {
  AggregateFunc* agg_func = nullptr;
  if (IsStringType(storage_type)) {
    agg_func = KNEW LastAggregate<true>(col_index, col_id, ts_col_id, time, len + STRING_WIDE);
  } else if (storage_type == roachpb::DataType::DECIMAL) {
    agg_func = KNEW LastAggregate<>(col_index, col_id, ts_col_id, time, len + BOOL_WIDE);
  } else {
    agg_func = KNEW LastAggregate<>(col_index, col_id, ts_col_id, time, len);
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateLastTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                                  k_uint32 ts_col_id, k_int64 time) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW LastTSAggregate(col_index, col_id, ts_col_id, time, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateLastRow(roachpb::DataType storage_type, k_int32 col_index,
                                                                   k_int32 col_id, k_uint32 len, k_uint32 ts_col_id) {
  AggregateFunc* agg_func = nullptr;
  if (IsStringType(storage_type)) {
    agg_func = KNEW LastRowAggregate<true>(col_index, col_id, ts_col_id, len + STRING_WIDE);
  } else if (storage_type == roachpb::DataType::DECIMAL) {
    agg_func = KNEW LastRowAggregate<>(col_index, col_id, ts_col_id, len + BOOL_WIDE);
  } else {
    agg_func = KNEW LastRowAggregate<>(col_index, col_id, ts_col_id, len);
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateLastRowTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                                     k_uint32 ts_col_id) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW LastRowTSAggregate(col_index, col_id, ts_col_id, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateFirst(roachpb::DataType storage_type, k_int32 col_index,
                                                                k_int32 col_id, k_uint32 len, k_uint32 ts_col_id) {
  AggregateFunc* agg_func = nullptr;
  if (IsStringType(storage_type)) {
    agg_func = KNEW FirstAggregate<true>(col_index, col_id, ts_col_id, len + STRING_WIDE);
  } else if (storage_type == roachpb::DataType::DECIMAL) {
    agg_func = KNEW FirstAggregate<>(col_index, col_id, ts_col_id, len + BOOL_WIDE);
  } else {
    agg_func = KNEW FirstAggregate<>(col_index, col_id, ts_col_id, len);
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateFirstTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                                   k_uint32 ts_col_id) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW FirstTSAggregate(col_index, col_id, ts_col_id, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateFirstRow(roachpb::DataType storage_type, k_int32 col_index,
                                                                   k_int32 col_id, k_uint32 len, k_uint32 ts_col_id) {
  AggregateFunc* agg_func = nullptr;
  if (IsStringType(storage_type)) {
    agg_func = KNEW FirstRowAggregate<true>(col_index, col_id, ts_col_id, len + STRING_WIDE);
  } else if (storage_type == roachpb::DataType::DECIMAL) {
    agg_func = KNEW FirstRowAggregate<>(col_index, col_id, ts_col_id, len + BOOL_WIDE);
  } else {
    agg_func = KNEW FirstRowAggregate<>(col_index, col_id, ts_col_id, len);
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateFirstRowTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                                      k_uint32 ts_col_id) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW FirstRowTSAggregate(col_index, col_id, ts_col_id, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateSTDDEVRow(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW STDDEVRowAggregate(col_index, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateAVGRow(roachpb::DataType storage_type, k_int32 col_index,
                                                                  k_int32 col_id, k_uint32 len) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW AVGRowAggregate<k_int16>(col_index, col_id, len);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW AVGRowAggregate<k_int32>(col_index, col_id, len);
      break;
    case roachpb::DataType::BIGINT:
      agg_func = KNEW AVGRowAggregate<k_int64>(col_index, col_id, len);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW AVGRowAggregate<k_float32>(col_index, col_id, len);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW AVGRowAggregate<k_double64>(col_index, col_id, len);
      break;
    default:
      LOG_ERROR("unsupported data type for sum aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for sum aggregation");
      break;
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateTwa(roachpb::DataType storage_type, k_int32 col_index,
                                                               k_int32 col_id, k_uint32 len, k_uint32 ts_col_id,
                                                               k_double64 const_val) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW TwaAggregate<k_int16>(col_index, col_id, ts_col_id, const_val, len);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW TwaAggregate<k_int32>(col_index, col_id, ts_col_id, const_val, len);
      break;
    case roachpb::DataType::BIGINT:
      agg_func = KNEW TwaAggregate<k_int64>(col_index, col_id, ts_col_id, const_val, len);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW TwaAggregate<k_float32>(col_index, col_id, ts_col_id, const_val, len);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW TwaAggregate<k_double64>(col_index, col_id, ts_col_id, const_val, len);
      break;
    default:
      LOG_ERROR("unsupported data type for twa aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for twa aggregation");
      break;
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateElapsed(roachpb::DataType storage_type, k_int32 col_index,
                                                                   k_int32 col_id, k_uint32 len, std::string& time) {
  AggregateFunc* agg_func = nullptr;
  agg_func = KNEW ElapsedAggregate(col_index, col_id, time, storage_type, len);
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateMaxExtend(roachpb::DataType storage_type, k_int32 col_index,
                                                                     k_int32 col_id, k_uint32 len2, k_int32 col_id2,
                                                                     bool is_string) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::BOOL:
      agg_func = KNEW MaxExtendAggregate<k_bool>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW MaxExtendAggregate<k_int16>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW MaxExtendAggregate<k_int32>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT:
      agg_func = KNEW MaxExtendAggregate<k_int64>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW MaxExtendAggregate<k_float32>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW MaxExtendAggregate<k_double64>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY:
      agg_func = KNEW MaxExtendAggregate<String>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::DECIMAL:
      agg_func = KNEW MaxExtendAggregate<k_decimal>(col_index, col_id, len2, col_id2, is_string);
      break;
    default:
      LOG_ERROR("unsupported data type for max aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for max aggregation");
      break;
  }
  return agg_func;
}

AggregateFunc* AggregateFuncFactory::CreateMinExtend(roachpb::DataType storage_type, k_int32 col_index,
                                                                     k_int32 col_id, k_uint32 len2, k_int32 col_id2,
                                                                     bool is_string) {
  AggregateFunc* agg_func = nullptr;
  switch (storage_type) {
    case roachpb::DataType::BOOL:
      agg_func = KNEW MinExtendAggregate<k_bool>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::SMALLINT:
      agg_func = KNEW MinExtendAggregate<k_int16>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::INT:
      agg_func = KNEW MinExtendAggregate<k_int32>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT:
      agg_func = KNEW MinExtendAggregate<k_int64>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::FLOAT:
      agg_func = KNEW MinExtendAggregate<k_float32>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::DOUBLE:
      agg_func = KNEW MinExtendAggregate<k_double64>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY:
      agg_func = KNEW MinExtendAggregate<String>(col_index, col_id, len2, col_id2, is_string);
      break;
    case roachpb::DataType::DECIMAL:
      agg_func = KNEW MinExtendAggregate<k_decimal>(col_index, col_id, len2, col_id2, is_string);
      break;
    default:
      LOG_ERROR("unsupported data type for max aggregation\n");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for max aggregation");
      break;
  }
  return agg_func;
}

}   // namespace kwdbts
