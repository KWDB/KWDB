// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_internal_type.h"
#include "lg_api.h"
#include "ee_field.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

KStatus GetInternalField(const char *buf, k_uint32 length, Field **field, k_int32 seq) {
  k_uint32 internal_type = 0;
  k_uint32 width = 0;
  k_int32 precision = 0;
  for (k_uint32 i = 0; i < length; ) {
    k_uint64 wire = 0;
    for (k_uint32 shift = 0; ; shift += 7) {
      if (shift >= 64) {
        return KStatus::FAIL;
      }

      if (i >= length) {
        return KStatus::FAIL;
      }

      char byte = buf[i];
      ++i;
      wire |= (static_cast<k_uint64>(byte) & 0x7F) << shift;
      if (wire < 0x80) {
        break;
      }
    }

    k_uint32 fieldNum = wire >> 3;
    if (fieldNum <= 0) {
      // LOG_ERROR("proto: InternalType: illegal tag %u (wire type %llu)", fieldNum, wire);
      return KStatus::FAIL;
    }
    if (fieldNum > 2) {
      break;
    }
    k_uint64 wireType = wire & 0x7;

    switch (fieldNum) {
      case 1: {
        for (k_uint32 j = 0; ; j += 7) {
          if (j >= 64) {
            return KStatus::FAIL;
          }

          if (i >= length) {
            return KStatus::FAIL;
          }
          char byte = buf[i];
          ++i;
          internal_type |= (static_cast<k_uint32>(byte) & 0x7F) << j;
          if (byte < 0x80) {
            break;
          }
        }
      }
      break;
      case 2: {
        for (k_uint32 j = 0; ; j += 7) {
          if (j >= 64) {
            return KStatus::FAIL;
          }

          if (i >= length) {
            return KStatus::FAIL;
          }
          char byte = buf[i];
          ++i;
          width |= (static_cast<k_uint32>(byte) & 0x7F) << j;
          if (byte < 0x80) {
            break;
          }
        }
      }
      break;
      default:
        break;
    }

    if (KWDBTypeFamily::IntFamily == internal_type || KWDBTypeFamily::FloatFamily == internal_type) {
      continue;
    } else {
      break;
    }
  }

  switch (internal_type) {
    case KWDBTypeFamily::BoolFamily:
      *field = new FieldBool(seq, roachpb::DataType::BOOL, 1);
      break;
    case KWDBTypeFamily::IntFamily:
      if (16 == width) {
        *field = new FieldShort(seq, roachpb::DataType::SMALLINT, 2);
      } else if (32 == width) {
        *field = new FieldInt(seq, roachpb::DataType::INT, 4);
      } else if (64 == width) {
        *field = new FieldLonglong(seq, roachpb::DataType::BIGINT, 8);
      } else {
        LOG_ERROR("proto: InternalType IntFamily: illegal width %d", width);
        return KStatus::FAIL;
      }
      break;
    case KWDBTypeFamily::FloatFamily:
      if (32 == width) {
        *field = new FieldFloat(seq, roachpb::DataType::FLOAT, 4);
      } else if (64 == width) {
        *field = new FieldDouble(seq, roachpb::DataType::DOUBLE, 8);
      } else {
        LOG_ERROR("proto: InternalType FloatFamily: illegal width %d", width);
        return KStatus::FAIL;
      }
      break;
    case KWDBTypeFamily::DecimalFamily:
      *field = new FieldDecimal(seq, roachpb::DataType::DECIMAL, 0);
      break;
    case KWDBTypeFamily::DateFamily:
      *field = new FieldLonglong(seq, roachpb::DataType::DATE, 0);
      break;
    case KWDBTypeFamily::TimestampFamily:
      *field = new FieldLonglong(seq, roachpb::DataType::TIMESTAMP, 8);
      break;
    case KWDBTypeFamily::IntervalFamily:
      *field = new FieldLonglong(seq, roachpb::DataType::TIMESTAMPTZ, 8);
      break;
    case KWDBTypeFamily::StringFamily:
      *field = new FieldVarchar(seq, roachpb::DataType::VARCHAR, 0);
      break;
    case KWDBTypeFamily::BytesFamily: {
      *field = new FieldVarBlob(seq, roachpb::DataType::VARBINARY, 0);
      break;
    case KWDBTypeFamily::TimestampTZFamily:
      *field = new FieldLonglong(seq, roachpb::DataType::TIMESTAMPTZ, 8);
      break;
    default:
      LOG_ERROR("proto: InternalType: illegal internal_type %d", internal_type);
      return KStatus::FAIL;
    }
  }

  if (*field) {
    (*field)->is_chunk_ = true;
    (*field)->set_return_type(static_cast<KWDBTypeFamily>(internal_type));
    (*field)->set_column_type(::roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
  }

  return KStatus::SUCCESS;
}

}   // namespace kwdbts

