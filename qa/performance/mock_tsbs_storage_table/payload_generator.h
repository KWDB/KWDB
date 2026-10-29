#pragma once
#include "data_type.h"

// specialize
template<typename T, typename = typename std::enable_if_t<
    std::is_integral<std::remove_reference_t<T>>::value, int>>
// int8/16/32/64, bool, char...
static size_t copy(uint8_t* dst, T&& arg, size_t width) {
  switch (width) {
    case 1: {
      auto n = static_cast<uint8_t>(arg);
      memcpy(dst, &n, width);
      break;
    }
    case 2: {
      auto n = static_cast<uint16_t>(arg);
      memcpy(dst, &n, width);
      break;
    }
    case 4: {
      auto n = static_cast<uint32_t>(arg);
      memcpy(dst, &n, width);
      break;
    }
    case 8: {
      auto n = static_cast<uint64_t>(arg);
      memcpy(dst, &n, width);
      break;
    }
    default:
      cerr << "INVALID TYPE" << endl;
      memcpy(dst, &arg, width);
      break;
  }
  return width;
}

// for float and double
static size_t copy(uint8_t* dst, double arg, size_t width) {
  if (width == 4) {
    auto f = static_cast<float>(arg);
    memcpy(dst, &f, width);
  } else {
    memcpy(dst, &arg, width);
  }
  return width;
}

// for const char *
static size_t copy(uint8_t* dst, const char* arg, size_t width) {
  width = std::min(width, strlen(arg));
  memcpy(dst, arg, width);
  return width;
}

// for const string &
static size_t copy(uint8_t* dst, const string& arg, size_t width) {
  width = std::min(width, arg.length());
  memcpy(dst, arg.c_str(), width);
  return width;
}

// for string_view
static size_t copy(uint8_t* dst, string_view arg, size_t width) {
  width = std::min(width, arg.length());
  memcpy(dst, arg.data(), width);
  return width;
}

class TagPayloadGenerator final {
 public:
  /**
   * @brief constructor
   * @param schema tag table schema
   */
  explicit TagPayloadGenerator(vector<TagInfo> schema)
      : schema_(std::move(schema)),
        payload_sz_(0),
        bitmap_sz_((schema_.size() + 7) >> 3) {
    payload_sz_ = bitmap_sz_;
    // update offset and payload_sz_
    uint32_t last_off = 0;
    for (auto& c : schema_) {
      c.m_offset = last_off;
      if (c.m_data_type == DATATYPE::VARSTRING || c.m_data_type == DATATYPE::VARBINARY) {
        if (c.m_tag_type == PRIMARY_TAG) {
          last_off += c.m_length;
          payload_sz_ += c.m_length;
        } else {
          last_off += sizeof(int64_t);
          payload_sz_ += c.m_length + 10;  // 8byte offset + 2byte len
        }
      } else {
        last_off += c.m_length;
        payload_sz_ += c.m_length;
      }
    }
  }

  /**
   * @brief construct a payload with params
   * @tparam T column value types
   * @param bitmap null bitmap
   * @param args column values
   * @return payload addr
   */
  template<typename... T>
  uint8_t* construct(uint8_t* bitmap, T&& ... args) const {
    // allocate
    auto payload_ptr = new uint8_t[payload_sz_];
    memset(payload_ptr, 0, payload_sz_);
    // construct
    auto last_width = schema_.back().m_length;
    if ((schema_.back().m_data_type == DATATYPE::VARSTRING || schema_.back().m_data_type == DATATYPE::VARBINARY) &&
        schema_.back().m_tag_type == GENERAL_TAG) {
      last_width = sizeof(int64_t);
    }
    auto varchar_ptr = payload_ptr + bitmap_sz_ + schema_.back().m_offset + last_width;
    if (bitmap) {
      memcpy(payload_ptr, bitmap, bitmap_sz_);
    }
    do_payload<0>(payload_ptr, varchar_ptr, std::forward<T>(args)...);
    return payload_ptr;
  }

  /**
   * @brief destroy a payload
   * @param payload payload addr
   */
  static void destroy(const uint8_t* payload) {
    delete[] payload;
  }

  /**
   * @brief payload size
   * @return payload size
   */
  [[nodiscard]] size_t payload_size() const { return payload_sz_; }

  /**
   * @brief return schema of generator
   * @return schema
   */
  [[nodiscard]] const vector<TagInfo>& schema() const { return schema_; }

 private:
  template<size_t CI, typename T, typename... Y>
  void do_payload(uint8_t* payload_ptr, uint8_t*& varchar_ptr, T&& t, Y&& ... y) const {
    if (CI >= schema_.size()) {
      return;
    }

    // null bitmap
    if (!get_null_bitmap(payload_ptr, CI)) {
      auto buff = payload_ptr + bitmap_sz_ + schema_.at(CI).m_offset;

      switch (schema_.at(CI).m_data_type) {
        case DATATYPE::K_BOOL:
        case DATATYPE::BYTE:
        case DATATYPE::K_INT8:
        case DATATYPE::K_INT16:
        case DATATYPE::K_INT32:
        case DATATYPE::TIMESTAMP:
        case DATATYPE::K_INT64:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::K_FLOAT:
        case DATATYPE::K_CHAR:
        case DATATYPE::BINARY:
        case DATATYPE::K_DOUBLE: {
          copy(buff, std::forward<T>(t), schema_.at(CI).m_length);
          return do_payload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
        }
        case DATATYPE::VARBINARY:
        case DATATYPE::VARSTRING: {
          if (schema_.at(CI).m_tag_type == PRIMARY_TAG) {
            copy(buff, std::forward<T>(t), schema_.at(CI).m_length);
            return do_payload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
          }
          // offset
          uint64_t off = varchar_ptr - payload_ptr;
          memcpy(buff, &off, 8);
          // varchar data
          uint16_t var_len = copy(varchar_ptr + 2, std::forward<T>(t), schema_.at(CI).m_length);
          memcpy(varchar_ptr, &var_len, sizeof(var_len));
          varchar_ptr += var_len + 2;
          // next column
          return do_payload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
        }
        default:
          cerr << "UNEXPECTED DATATYPE: " << schema_.at(CI).m_data_type << endl;
          break;  // do nothing
      }
    }
    return do_payload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
  }

  template<size_t CI, typename T>
  void do_payload(uint8_t* payload_ptr, uint8_t*& varchar_ptr, T&& t) const {
    if (get_null_bitmap(payload_ptr, CI)) {
      return;
    }
    auto buff = payload_ptr + bitmap_sz_ + schema_.at(CI).m_offset;
    // not varchar
    if (schema_.at(CI).m_data_type != DATATYPE::VARSTRING && schema_.at(CI).m_data_type != DATATYPE::VARBINARY) {
      copy(buff, std::forward<T>(t), schema_.at(CI).m_length);
      return;
    }
    // primary varchar
    if (schema_.at(CI).m_tag_type == (PRIMARY_TAG)) {
      copy(buff, std::forward<T>(t), schema_.at(CI).m_length);
      return;
    }
    // offset
    uint64_t off = varchar_ptr - payload_ptr;
    memcpy(buff, &off, 8);
    // varchar data
    uint16_t var_len = copy(varchar_ptr + 2, std::forward<T>(t), schema_.at(CI).m_length);
    memcpy(varchar_ptr, &var_len, sizeof(var_len));
  }

 private:
  vector<TagInfo> schema_;
  size_t payload_sz_;
  size_t bitmap_sz_;
};

class PayloadGenerator final {
 public:
  explicit PayloadGenerator(const vector<AttributeInfo>& schema)
    : metric_schema_(schema) {
    //
  }

  template <typename... Args>
  std::pair<uint8_t *, uint32_t> construct(const uint8_t * tag_data, uint32_t tag_len, uint32_t primary_offset, uint16_t primary_len, // tag
                      int32_t row_cnt, uint8_t * null_bitmap, Args&&... args) {
    row_cnt = row_cnt <= 0 ? 1 : row_cnt;
    set_row_cnt(row_cnt);
    // length
    int32_t bitmap_len = (row_cnt + 7) >> 3;
    int32_t var_len = 0;
    int32_t var_offset = 0;
    for (auto & c : metric_schema_) {
      c.offset = var_offset;
      var_offset += c.size * row_cnt + bitmap_len;
      if (c.type == DATATYPE::VARBINARY || c.type == DATATYPE::VARSTRING) {
        var_len += c.length + 2;
      }
    }
    auto metric_len = var_offset + var_len * 2;
    auto total_len = HEADER_SIZE + 2 + primary_len + 4 + tag_len + 4 + metric_len;
    auto payload_ptr = new uint8_t[total_len];
    memset(payload_ptr, 0, total_len);
    auto buff = payload_ptr;
    // HEADER
    memcpy(buff, reinterpret_cast<uint8_t *>(&txn_), 16); buff += 16;
    memcpy(buff, &group_id_, 2); buff += 2;
    memcpy(buff, &version_, 4); buff += 4;
    memcpy(buff, &db_id_, 4); buff += 4;
    memcpy(buff, &tb_id_, 8); buff += 8;
    memcpy(buff, &row_cnt, 4); buff += 4;
    memcpy(buff, &flag_, 1); buff += 1;
    // PRIMARY LEN
    memcpy(buff, &primary_len, 2); buff += 2;
    // PRIMARY DATA
    memcpy(buff, tag_data + primary_offset, primary_len); buff += primary_len;
    // TAG LEN
    memcpy(buff, &tag_len, 4); buff += 4;
    // TAG DATA
    memcpy(buff, tag_data, tag_len); buff += tag_len;
    // METRIC LEN
    memcpy(buff, &metric_len, 4); buff += 4;
    // METRIC DATA
    // varchar offset
    auto varchar_ptr = buff + var_offset;
    // construct metric payload
    do_payload<0>(null_bitmap, buff, varchar_ptr, std::forward<Args>(args)...);
    // cout << "Payload length: " << total_len << endl;
    return {payload_ptr, total_len};
  }

  static void destroy(const uint8_t * payload) {
    delete[] payload;
  }

  [[nodiscard]] const vector<AttributeInfo>& schema() const { return metric_schema_; }

  inline void set_txn(uint8_t * txn) { if (txn) { memcpy(reinterpret_cast<uint8_t *>(&txn_), txn, 16); }}
  inline void set_group(uint16_t group) { group_id_ = group; }
  inline void set_version(uint32_t ver) { version_ = ver; }
  inline void set_db(uint32_t db) { db_id_ = db; }
  inline void set_tb(uint64_t tb) { tb_id_ = tb; }
  inline void set_row_cnt(uint32_t rc) { row_cnt_ = rc; }
  inline void set_flag(uint8_t flag) { flag_ = flag; }

 private:
  template <size_t CI, typename T, typename... Y>
  void do_payload(uint8_t * null_bitmap, uint8_t * payload_ptr, uint8_t *& varchar_ptr, T&& t, Y&&... y) const {
    if (CI > metric_schema_.size()) {
      return;
    }
    auto buff = payload_ptr + metric_schema_.at(CI).offset;
    if (null_bitmap && get_null_bitmap(null_bitmap, CI)) {
      memset(buff, -1, ((row_cnt_ + 7) >> 3));
    } else {
      auto data_buff = buff + ((row_cnt_ + 7) >> 3);
      if (metric_schema_.at(CI).type == DATATYPE::VARSTRING || metric_schema_.at(CI).type == DATATYPE::VARBINARY) {
        for (size_t i = 0; i < row_cnt_; ++i) {
          // write offset
          uint64_t off = varchar_ptr - buff;
          copy(data_buff, off, metric_schema_.at(CI).size);
          // write varchar
          uint16_t len = copy(varchar_ptr + 2, std::forward<T>(t), metric_schema_.at(CI).length);
          memcpy(varchar_ptr, &len, sizeof(len));
          varchar_ptr += len + sizeof(len);
          data_buff += metric_schema_.at(CI).size;
        }
      } else {
        for (size_t i = 0; i < row_cnt_; ++i) {
          copy(data_buff, std::forward<T>(t), metric_schema_.at(CI).size);
          data_buff += metric_schema_.at(CI).size;
        }
      }
    }
    do_payload<CI + 1>(null_bitmap, payload_ptr, varchar_ptr, std::forward<Y>(y)...);
  }

  template <size_t CI, typename T>
  void do_payload(uint8_t * null_bitmap, uint8_t * payload_ptr, uint8_t *& varchar_ptr, T&& t) const {
    auto buff = payload_ptr + metric_schema_.at(CI).offset;
    if (null_bitmap && get_null_bitmap(null_bitmap, CI)) {
      memset(buff, -1, ((row_cnt_ + 7) >> 3));
    } else {
      auto data_buff = buff + ((row_cnt_ + 7) >> 3);
      if (metric_schema_.at(CI).type == DATATYPE::VARSTRING || metric_schema_.at(CI).type == DATATYPE::VARBINARY) {
        for (size_t i = 0; i < row_cnt_; ++i) {
          // write offset
          uint64_t off = varchar_ptr - buff;
          copy(data_buff + i * metric_schema_.at(CI).size, off, metric_schema_.at(CI).size);
          // write varchar
          uint16_t len = copy(varchar_ptr + 2, std::forward<T>(t), metric_schema_.at(CI).length);
          memcpy(varchar_ptr, &len, sizeof(len));
          varchar_ptr += len + sizeof(len);
        }
      } else {
        copy(data_buff, std::forward<T>(t), metric_schema_.at(CI).size);
      }
    }
  }

  constexpr static uint64_t HEADER_SIZE = 16 + 2 + 4 + 4 + 8 + 4 + 1;

 private:
  vector<AttributeInfo> metric_schema_;
  std::aligned_storage<16, 8>::type txn_{}; // 16 byte
  uint16_t group_id_{}; // 2 byte
  uint32_t version_{};  // 4 byte
  uint32_t db_id_{};  // 4 byte
  uint64_t tb_id_{};  // 8 byte
  uint32_t row_cnt_{};  // 4 byte
  uint8_t flag_{};  // 1 byte
  //
};
