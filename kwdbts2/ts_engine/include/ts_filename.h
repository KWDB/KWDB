#pragma once
#include <cstdint>
#include <string>

#include "data_type.h"
namespace kwdbts {
inline std::string LastSegmentFileName(uint64_t file_number) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "V2last.ver-%012lu", file_number);
  return buffer;
}

inline std::string VGroupDirName(uint32_t vgroup_id) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "vg_%03u", vgroup_id);
  return buffer;
}

inline std::string PartitionDirName(uint32_t database_id, timestamp64 start) {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "V2db%02d-%014ld", database_id, start);
  return buffer;
}
};  // namespace kwdbts