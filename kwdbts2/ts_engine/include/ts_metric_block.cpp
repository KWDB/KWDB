#include "ts_metric_block.h"

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"

namespace kwdbts {
KStatus TsMetricBlockBuilder::PutBlockSpan(std::shared_ptr<TsBlockSpan> span) {
  for (int icol = 0; icol < col_schemas_.size(); icol++) {
    if (isVarLenType(col_schemas_[icol].type)) {
      // looping row by row to copy data
      for (int irow = 0; irow < span->GetRowNum(); irow++) {
        DataFlags flag;
        TSSlice data;
        auto s =
            span->GetVarLenTypeColAddr(irow, icol, col_schemas_, col_schemas_[icol], flag, data);
        if (s == FAIL) {
          return s;
        }
        column_block_builders_[icol]->AppendVarLenData(data, flag);
      }
    }
  }
  return SUCCESS;
}
}  // namespace kwdbts