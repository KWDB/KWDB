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

#include "st_wal_internal_logfile_mgr.h"

#include <utility>
#include "sys_utils.h"

extern int g_engine_version;

namespace kwdbts {

WALFileMgr::WALFileMgr(string wal_path, const KTableKey table_id, EngineOptions* opt, bool read_chk)
    : wal_path_(std::move(wal_path)), table_id_(table_id), opt_(opt), read_chk_(read_chk) {
  file_mutex_ = new WALFileMgrFileLatch(LATCH_ID_WALFILEMGR_FILE_MUTEX);
}

WALFileMgr::~WALFileMgr() {
  if (file_.is_open()) {
    file_.close();
  }
  if (file_mutex_ != nullptr) {
    delete file_mutex_;
    file_mutex_ = nullptr;
  }
}

KStatus WALFileMgr::Open() {
  string path = getFilePath();
  if (IsExists(path)) {
    if (file_.is_open()) {
      header_block_ = readHeaderBlock();
      return SUCCESS;
    } else {
      file_.close();
    }

    file_.open(path, std::ios::in | std::ios::out);
    if (file_.is_open()) {
      header_block_ = readHeaderBlock();
      return SUCCESS;
    }
  }
  LOG_ERROR("Failed to open the WAL log file from %s", path.c_str())
  return FAIL;
}

KStatus WALFileMgr::Close() {
  if (file_.is_open()) {
      file_.close();
  }
  return SUCCESS;
}

KStatus WALFileMgr::initWalFile(TS_LSN first_lsn, TS_LSN flush_lsn) {
  HeaderBlock header = HeaderBlock(table_id_, 0, opt_->GetBlockNumPerFile(),
                                   0, first_lsn, first_lsn, 0);
  return initWalFileWithHeader(header);
}

KStatus WALFileMgr::initWalFileWithHeader(HeaderBlock& header) {
  // create the wal directory if it doesn't exist
  if (!IsExists(wal_path_)) {
    MakeDirectory(wal_path_);
  }

  string path = getFilePath();
  if (file_.is_open()) {
    // current log file is full. flush and close it.
    file_.close();
  }

  // check the wal log file, if it doesn't exist, initialize a new one.
  if (!IsExists(path)) {
    file_.open(path, std::ios::in | std::ios::out | std::ios::trunc);
    if (file_.fail()) {
      // failed to open the new log file, report an error.
      LOG_ERROR("Failed to open the WAL log file from %s", path.c_str())
      return FAIL;
    }
    char* header_value = header.encode();
    file_.write(header_value, BLOCK_SIZE);

    file_.seekg(BLOCK_SIZE, std::ios::beg);
    EntryBlock eb = EntryBlock();
    eb.reset(0);
    char* eb_value = eb.encode();
    file_.write(eb_value, BLOCK_SIZE);
    file_.flush();

    delete[] header_value;
    delete[] eb_value;
  } else {
      // we should check the header of the existing log file to ensure it's old enough to been overwritten.
      HeaderBlock old_header = getHeader();
      if (old_header.getCheckpointNo() == header.getCheckpointNo()) {
        LOG_ERROR("Failed to init the WAL log file from %s, require checkpoint first", path.c_str())
        return FAIL;
      }

    file_.open(path, std::ios::in | std::ios::out | std::ios::trunc);
    char* header_value = header.encode();
    file_.write(header_value, BLOCK_SIZE);
    delete[] header_value;

    file_.seekg(BLOCK_SIZE, std::ios::beg);
    EntryBlock eb = EntryBlock();
    eb.reset(0);
    char* eb_value = eb.encode();
    file_.write(eb_value, BLOCK_SIZE);
    delete[] eb_value;

    file_.flush();
  }

  header_block_ = header;
  return SUCCESS;
}

KStatus WALFileMgr::writeHeaderBlock(HeaderBlock& hb) {
  char* data = hb.encode();
  file_.seekg(0, std::ios::beg);
  file_.write(data, BLOCK_SIZE);
  delete[] data;
  if (file_.fail()) {
    LOG_ERROR("Failed to write the WAL log file_.")
    return FAIL;
  }
  return SUCCESS;
}

KStatus WALFileMgr::writeBlocks(std::vector<EntryBlock*>& entry_blocks, HeaderBlock& header, bool flush_header) {
  if (entry_blocks.empty()) {
    return SUCCESS;
  }
  EntryBlock* first_block = entry_blocks[0];

  uint64_t offset = (first_block->getBlockNo() - header.getStartBlockNo() + 1) * BLOCK_SIZE;
  file_.seekp(offset, std::ios::beg);

  for (auto entry_block : entry_blocks) {
    char* data = entry_block->encode();
    file_.write(data, BLOCK_SIZE);
    delete[] data;
    if (file_.fail()) {
      LOG_ERROR("Failed to write the WAL log file_.")
      return FAIL;
    }

    if (entry_block->getBlockNo() != 0 &&
        entry_block->getBlockNo() == header.getEndBlockNo() &&
        entry_block->getDataLen() == LOG_BLOCK_MAX_LOG_SIZE) {
      writeHeaderBlock(header);
      file_.flush();

      TS_LSN start_lsn = header.getStartLSN() + BLOCK_SIZE + header.getBlockNum() * BLOCK_SIZE;
      TS_LSN first_lsn = start_lsn + BLOCK_SIZE + LOG_BLOCK_HEADER_SIZE + entry_block->getFirstRecOffset();
      header = HeaderBlock(table_id_, entry_block->getBlockNo() + 1, opt_->GetBlockNumPerFile(), start_lsn, first_lsn,
                           header.getCheckpointLSN(), header.getCheckpointNo());

      if (initWalFileWithHeader(header) == FAIL) {
        LOG_ERROR("Failed init WAL log file %s", getFilePath().c_str())
        return FAIL;
      }
      file_.seekp(BLOCK_SIZE, std::ios::beg);
    }
  }
  if (flush_header) {
    writeHeaderBlock(header);
  }
  if (opt_->wal_level == WALMode::SYNC) {
    auto helper = [](std::filebuf *fb) -> int {
      class Helper : public std::filebuf {
       public:
        int handle() { return _M_file.fd(); }
      };
      return static_cast<Helper*>(fb)->handle();
    };
    fsync(helper(file_.rdbuf()));
  } else {
    file_.flush();
  }
  return SUCCESS;
}

HeaderBlock WALFileMgr::readHeaderBlock() {
  char* data = KNEW char[BLOCK_SIZE];
  file_.seekg(0, std::ios::beg);
  file_.read(data, BLOCK_SIZE);
  auto header = HeaderBlock(data);
  delete[] data;
  return header;
}

KStatus WALFileMgr::readEntryBlocks(std::vector<EntryBlock*>& entry_blocks,
                                 uint64_t start_block_no, uint64_t end_block_no) {
  KStatus s = SUCCESS;
  std::ifstream wal_file;
  HeaderBlock header = header_block_;
  std::string file_path = getFilePath();
  uint64_t min_block_no = header.getStartBlockNo() + 1;
  while (start_block_no < header.getStartBlockNo()) {
    if (header.getStartBlockNo() < min_block_no) {
      min_block_no = header.getStartBlockNo();
    } else {
      start_block_no = min_block_no;
      break;
    }

    if (wal_file.is_open()) {
      wal_file.close();
    }

    file_path = getFilePath();
    wal_file.open(file_path, std::ios::binary);
    if (!wal_file.is_open()) {
      entry_blocks.clear();
      return FAIL;
    }

    char* data = KNEW char[BLOCK_SIZE];
    wal_file.seekg(0, std::ios::beg);
    wal_file.read(data, BLOCK_SIZE);
    header = HeaderBlock(data);
    delete[] data;
  }

  uint64_t offset = (start_block_no - header.getStartBlockNo() + 1) * BLOCK_SIZE;
  if (!wal_file.is_open()) {
    wal_file.open(file_path, std::ios::binary);
  }

  wal_file.seekg(offset, std::ios::beg);

  char* data = KNEW char[BLOCK_SIZE];

  for (uint64_t index = start_block_no; index <= end_block_no; index++) {
    if (!wal_file.read(data, BLOCK_SIZE)) {
      wal_file.close();

      if (index + 1 > header.getStartBlockNo() + header.getBlockNum()) {
        file_path = getFilePath();
        wal_file.open(file_path, std::ios::binary);
        wal_file.seekg(BLOCK_SIZE, std::ios::beg);
      }

      if (!wal_file.is_open()) {
        s = FAIL;
        break;
      }

      // read the block again (from the new opened wal file)
      wal_file.read(data, BLOCK_SIZE);
    }
    auto* entry_block = KNEW EntryBlock(data);

    /*
    // In consideration of the rafe of writing, cancel checksum
    uint32_t checksum = entryBlock->getCheckSum();
    uint32_t compute_checksum = 0;
    for (int i = 0; i < BLOCK_SIZE - sizeof(entryBlock->getCheckSum()); i++) {
      compute_checksum += static_cast<uint8_t>(data[i]);
    }
    if (checksum != compute_checksum) {
      delete entryBlock;
      s = FAIL;
      break;
    }*/
    if (entry_block->getBlockNo() != index) {
      delete entry_block;
      s = FAIL;
      break;
    }
    entry_blocks.emplace_back(entry_block);
  }

  if (wal_file.is_open()) {
    wal_file.close();
  }

  delete[] data;

  return s;
}

void WALFileMgr::CleanUp(TS_LSN checkpoint_lsn, TS_LSN current_lsn) {
  if (checkpoint_lsn == current_lsn) {
    string path = getFilePath();
    if (IsExists(path)) {
      Remove(path);
    }
  }
}

KStatus WALFileMgr::ResetWALInternal(kwdbContext_p ctx, TS_LSN current_lsn_recover) {
  string path = getFilePath();
  if (IsExists(path)) {
    Remove(path);
  }
  TS_LSN first_lsn = BLOCK_SIZE + LOG_BLOCK_HEADER_SIZE;
  KStatus s = initWalFile(first_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL file.")
    return s;
  }
  return SUCCESS;
}

TS_LSN WALFileMgr::GetLSNFromBlockNo(uint64_t block_no) {
  HeaderBlock header = header_block_;
  if (block_no >= header.getStartBlockNo() && block_no <= header.getEndBlockNo()) {
    // at current file
    return header.getStartLSN() + (block_no - header.getStartBlockNo() + 1) * BLOCK_SIZE + LOG_BLOCK_HEADER_SIZE;
  }
  LOG_ERROR("Failed find WAL block %ld", block_no)
  return 0;
}

uint64_t WALFileMgr::GetBlockNoFromLsn(TS_LSN lsn) {
  if (lsn == 0) {
    return 0;
  }

  HeaderBlock header = header_block_;
  TS_LSN min_offset = header.getStartLSN();
  while (lsn < header.getStartLSN()) {
    if (header.getStartLSN() - lsn < min_offset) {
      min_offset = header.getStartLSN() - lsn;
    } else {
      return 0;
    }
    header = getHeader();
  }

  return (lsn - header.getStartLSN() - BLOCK_SIZE) / BLOCK_SIZE + header.getStartBlockNo();
}

HeaderBlock WALFileMgr::getHeader() {
  std::ifstream wal_file;
  std::string path;
  path = getFilePath();

  wal_file.open(path, std::ios::binary);
  if (!wal_file.is_open()) {
    LOG_ERROR("Failed to open the WAL log file %s.", path.c_str())
    return {};
  }

  char* data = KNEW char[BLOCK_SIZE];
  wal_file.seekg(0, std::ios::beg);
  wal_file.read(data, BLOCK_SIZE);
  HeaderBlock header(data);
  delete[] data;

  if (wal_file.is_open()) {
    wal_file.close();
  }

  return header;
}
}  // namespace kwdbts
