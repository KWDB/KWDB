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

#include <fstream>
#include <string>
#include <vector>

#include "st_wal_internal_logblock.h"
#include "st_wal_types.h"
#include "lt_rw_latch.h"

namespace kwdbts {

/**
* WAL Files Management class, manages WAL log group files and life cycle.
*/
class WALFileMgr {
 public:
  WALFileMgr() = delete;

  WALFileMgr(string wal_path, const KTableKey table_id, EngineOptions* opt, bool is_read_chk = false);

  ~WALFileMgr();

  KStatus Open();

  KStatus OpenTmp();

  /**
   * Init WAL file, and init HeaderBlock
   * @return
   */
  KStatus initWalFile(TS_OSN first_lsn, TS_OSN flush_lsn = 0, bool tmp_file = false);


  KStatus initWalFileWithHeader(HeaderBlock& header, bool tmp_file = false);

  /**
   * Close WAL file
   * @param ctx
   * @return
   */
  KStatus Close();

  /**
   * write multiple EntryBlocks into current Block.
   * @param entry_blocks
   * @param start_index
   * @return
   */
  KStatus writeBlocks(std::vector<EntryBlock*>& entry_blocks, HeaderBlock& header, bool flush_header);

  /**
   * Read HeaderBlock
   * @return
   */
  HeaderBlock readHeaderBlock();

  /**
   * Read EntryBlocks between start_block_no and end_block_no.
   * @param start_block_no
   * @param end_block_no
   * @return
   */
  KStatus readEntryBlocks(std::vector<EntryBlock*>& entry_blocks, uint64_t start_block_no, uint64_t end_block_no);

  void Lock() {
    MUTEX_LOCK(file_mutex_);
  }

  void Unlock() {
    MUTEX_UNLOCK(file_mutex_);
  }

  /**
  * Clean up expired files.
  */
  void CleanUp(TS_OSN checkpoint_lsn, TS_OSN current_lsn);

  /**
 * Reset WAL files.
 */
  KStatus ResetWALInternal(kwdbContext_p ctx, TS_OSN current_lsn);

  TS_OSN GetLSNFromBlockNo(uint64_t block_no);

  uint64_t GetBlockNoFromLsn(TS_OSN lsn);

 private:
  /**
   * Write single HeaderBlock into current LogFile.
   * @param headerBlock
   * @return
   */
  KStatus writeHeaderBlock(HeaderBlock& hb);

  HeaderBlock getHeader();

  // This mutex is used to protect the active log file for read/write mutual exclusion.
  using WALFileMgrFileLatch = KLatch;

  WALFileMgrFileLatch* file_mutex_;

  EngineOptions* opt_{nullptr};
  HeaderBlock header_block_{};

  KTableKey table_id_;
  string wal_path_;
  bool read_chk_;

  std::fstream file_;

 public:
  string getFilePath() {
    if (read_chk_) {
      return wal_path_ + "kwdb_wal.chk";
    }
    return wal_path_ + "kwdb_wal.cur";
  }
  string getTmpFilePath() {
    return wal_path_ + "kwdb_wal.tmp";
  }
  string getChkFilePath() {
    return wal_path_ + "kwdb_wal.chk";
  }
  string getChkMetaFilePath() {
    return wal_path_ + "kwdb_wal.meta";
  }

//  uint16_t getFileNoFromLSN(TS_OSN lsn) {
//    uint16_t file_no = lsn / (opt_->wal_file_size << 20);
//    file_no = file_no % opt_->wal_file_in_group;
//    return file_no;
//  }
};
}  // namespace kwdbts
