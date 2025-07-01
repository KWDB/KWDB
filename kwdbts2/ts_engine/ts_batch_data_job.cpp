#include "ts_batch_data_job.h"
#include "ts_engine.h"

namespace kwdbts {

TsReadBatchDataJob::TsReadBatchDataJob(TSEngineV2Impl* ts_engine, TSTableID table_id, uint32_t table_version,
                                       uint64_t begin_hash, uint64_t end_hash, KwTsSpan ts_span, uint64_t job_id,
                                       std::vector<std::pair<uint64_t, uint64_t>> entity_ids)
                                       : TsBatchDataJob(job_id), ts_engine_(ts_engine), table_id_(table_id),
                                       table_version_(table_version), begin_hash_(begin_hash), end_hash_(end_hash),
                                       ts_span_(ts_span), entity_ids_(entity_ids) {
  vgroups_ = ts_engine_->GetTsVGroups();
}

KStatus TsReadBatchDataJob::Read(TSSlice* data, int32_t* row_num) {

  return KStatus::SUCCESS;
}

void TsReadBatchDataJob::Finish() {

}

void TsReadBatchDataJob::Cancel() {

}

KStatus TsWriteBatchDataJob::Write(TSSlice* data, int32_t* row_num) {
  return KStatus::SUCCESS;
}

void TsWriteBatchDataJob::Finish() {

}

void TsWriteBatchDataJob::Cancel() {

}

}  // namespace kwdbts
