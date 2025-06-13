#pragma once
#include <utility>
#include <vector>
#include "data_type.h"
#include "test_util.h"

class PayloadGenerator final {
 public:
  /**
   * @brief constructor
   * @param schema tag table schema
   */
  explicit PayloadGenerator(vector<TagInfo> schema)
      : schema_(std::move(schema)),
        payload_sz_(0),
        bitmap_sz_((schema_.size() + 7) >> 3) {
    payload_sz_ = bitmap_sz_;
    // update offset and payload_sz_
    int32_t last_off = 0;
    int32_t ptagLength = 0;
    for (auto& c : schema_) {
      c.m_offset = last_off;
      if (c.m_data_type == DATATYPE::VARSTRING) {
        if (c.m_tag_type == PRIMARY_TAG) {
          last_off += c.m_length;
          payload_sz_ += c.m_length;
          ptagLength += c.m_length;
        } else {
          last_off += sizeof(intptr_t);  // getDataTypeSize(c.m_data_type);
          payload_sz_ += c.m_length + 10;  // 8byte offset + 2byte len
        }
      } else {
        last_off += c.m_size;  // getDataTypeSize(c.m_data_type);
        payload_sz_ += c.m_length;  // getDataTypeSize(c.m_data_type);
        if (c.m_tag_type == PRIMARY_TAG) {
          ptagLength += c.m_length;
        }
      }
    }
    primaryTagLen_ = ptagLength;
    LOG_INFO("primaryTagGenerater new hashpoint ptag %d", primaryTagLen_);
    primaryTag_ = new uint8_t[primaryTagLen_];
  }
  int32_t getPtagLen() {
    return primaryTagLen_;
  }
  uint32_t GetHashPoint() {
    hashpoint = GetConsistentHashId(reinterpret_cast<char*>(primaryTag_), primaryTagLen_, g_testcase_hash_num);
    LOG_INFO("primaryTagGenerater free hashpoint ptag %d", primaryTagLen_);
    return hashpoint;
  }
  ~PayloadGenerator(){
    delete[] primaryTag_;
  }
  /**
   * @brief construct a payload with params
   * @tparam T column value types
   * @param bitmap null bitmap
   * @param args column values
   * @return payload addr
   */
  template<typename... T>
  uint8_t* Construct(uint8_t* bitmap, T&& ... args) const {
    // allocate
    auto payload_ptr = new uint8_t[payload_sz_];
    memset(payload_ptr, 0, payload_sz_);
    // construct
    auto varchar_ptr = payload_ptr + bitmap_sz_ + schema_.back().m_offset +
        (schema_.back().m_tag_type == PRIMARY_TAG ?
          schema_.back().m_length : getDataTypeSize(schema_.back().m_data_type));
    if (bitmap) {
      memcpy(payload_ptr, bitmap, bitmap_sz_);
    }
    doPayload<0>(payload_ptr, varchar_ptr, std::forward<T>(args)...);

    return payload_ptr;
  }

  /**
   * @brief destroy a payload
   * @param payload payload addr
   */
  static void Destroy(const uint8_t* payload) {
    delete[] payload;
  }

  /**
   * @brief payload size
   * @return payload size
   */
  [[nodiscard]] size_t PayloadSize() const { return payload_sz_; }

  /**
   * @brief return schema of generator
   * @return schema
   */
  [[nodiscard]] const vector<TagInfo>& Schema() const { return schema_; }

 private:
  template<size_t CI, typename T, typename... Y>
  void doPayload(uint8_t* payload_ptr, uint8_t*& varchar_ptr, T&& t, Y&& ... y) const {
    if (CI >= schema_.size()) {
      return;
    }

    // null bitmap
    if (!get_null_bitmap(payload_ptr, CI)) {
      auto buff = payload_ptr + bitmap_sz_ + schema_.at(CI).m_offset;
      switch (schema_.at(CI).m_data_type) {
        case DATATYPE::BOOL:
        case DATATYPE::BYTE:
        case DATATYPE::INT8:
        case DATATYPE::INT16:
        case DATATYPE::INT32:
        case DATATYPE::TIMESTAMP:
        case DATATYPE::INT64:
        case DATATYPE::TIMESTAMP64:
        case DATATYPE::FLOAT:
        case DATATYPE::DOUBLE: {
          if (schema_.at(CI).m_tag_type == (PRIMARY_TAG)) {
            copy(primaryTag_, std::forward<T>(t), getDataTypeSize(schema_.at(CI).m_data_type));
          }
          copy(buff, std::forward<T>(t), getDataTypeSize(schema_.at(CI).m_data_type));
          return doPayload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
        }
        case DATATYPE::CHAR: {
          if (schema_.at(CI).m_tag_type == (PRIMARY_TAG)) {
            copy(primaryTag_, std::forward<T>(t), schema_.at(CI).m_length);
          }
          copy(buff, std::forward<T>(t), schema_.at(CI).m_length);
          return doPayload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
        }
        case DATATYPE::VARSTRING: {
          if (schema_.at(CI).m_tag_type == PRIMARY_TAG) {
            copy(buff, std::forward<T>(t), schema_.at(CI).m_length);
            copy(primaryTag_, std::forward<T>(t), schema_.at(CI).m_length);
            return doPayload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
          }
          // offset
          uint64_t off = varchar_ptr - payload_ptr;
          memcpy(buff, &off, 8);
          // varchar data
          uint16_t var_len = copy(varchar_ptr + 2, std::forward<T>(t), schema_.at(CI).m_length);
          memcpy(varchar_ptr, &var_len, sizeof(var_len));
          varchar_ptr += var_len + 2;
          // next column
          return doPayload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
        }
        default:
          cerr << "UNEXPECTED DATATYPE: " << schema_.at(CI).m_data_type << endl;
          break;  // do nothing
      }
    }
    return doPayload<CI + 1>(payload_ptr, varchar_ptr, std::forward<Y>(y)...);
  }

  template<size_t CI, typename T>
  void doPayload(uint8_t* payload_ptr, uint8_t*& varchar_ptr, T&& t) const {
    if (get_null_bitmap(payload_ptr, CI)) {
      return;
    }
    auto buff = payload_ptr + bitmap_sz_ + schema_.at(CI).m_offset;
    // not varchar
    if (schema_.at(CI).m_data_type != DATATYPE::VARSTRING) {
      copy(buff, std::forward<T>(t), getDataTypeSize(schema_.at(CI).m_data_type));
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

  // specialize
  template<typename T, typename = typename std::enable_if_t<
      std::is_integral<std::remove_reference_t<T>>::value, int>>
  // int8/16/32/64, bool, char...
  size_t copy(uint8_t* dst, T&& arg, size_t width) const {
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

 private:
  vector<TagInfo> schema_;
  size_t payload_sz_;
  size_t bitmap_sz_;
  uint32_t hashpoint;
  int32_t primaryTagLen_;
  uint8_t *primaryTag_;
};
