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

#include "duckdb.hpp"
#include "duckdb/engine/ap_conn_cache.h"
#include "duckdb/main/capi/capi_internal.hpp"

namespace kwdbts {

DConnCache::DConnCache() {}

void DConnCache::SetDBWrapper(duckdb::DatabaseWrapper* wrapper) {
  copyOfEngineDBWrapper = wrapper;
}

// return NULL if any error, otherwise the corresponding parent struct pointer
DConEntry * DConnCache::lookForEntryByAddr(duckdb::Connection* conn)
{
  // caller should have acquired lock
  DConEntry * p = NULL;
  for (int i=0; i<current_sz; i++){
    p = *(dConCache + i);
    if (p->conn == conn){
      break;
    }
  }
  return p;
}

// return NULL if any error
// otherwise
DConEntry * DConnCache::lookForValidEntry(k_uint64 sessionID, std::string dbName, std::string userName)
{
  // we are in the "look up" scenario, assuming caller holds the mutex
  DConEntry * p = NULL;
  for (int i=0; i<current_sz; i++){
    p = *(dConCache + i);
    if (CacheState::IDLE == p->status){
      break;
    }
  }
  return p;
}

// allocate memory, create connection, and populate the fields.
// return true if successful, false otherwise
bool DConnCache::createEntry(DConEntry ** entry, k_uint64 sessionID, std::string dbName, std::string userName)
{
  // allocate one 
  DConEntry * ent = new DConEntry{};
  if (NULL == ent){
    // error condition, warn and return NULL
    printf("Failed to allocate Connection Entry.\n");
    return NULL;
  }

  // get the real connection
  ent->conn = std::make_shared<duckdb::Connection>(*(copyOfEngineDBWrapper->database)).get();
  if (NULL == ent->conn ){
    return false;
  }

  // populate the struct
  ent->dbName = dbName;
  ent->user = userName;
  ent->sessionID = sessionID;
  ent->status = CacheState::IDLE;  
  *entry = ent;
  return true;
}

// assuming we are hold the mutex
// TODO: complicated logic to handle the global indexes or maps
bool DConnCache::addEntryToList(DConEntry * ent){
 // now we have a new entry, add it to the tail of the array
  dConCache[current_sz] = ent;
  current_sz ++;
  if (current_sz >= MAX_CACHE_ENTRY_NUM){
    printf("Connection entry number hits upper limit, forcing an assert.\n");
    assert(0);
    return false;
  }
  return true;
}

duckdb::Connection* DConnCache::GetOrAddConn(k_uint64 sessionID, std::string dbName, std::string userName)
{
  std::lock_guard<std::mutex> guard(cap_mux);  // take a object level lock

  DConEntry * result = lookForValidEntry(sessionID, dbName, userName);
  if (result) {return result->conn; }  

  if (false == createEntry(&result, sessionID, dbName, userName)){
    // failure in createEntry
    return NULL;
  }
  addEntryToList(result);

  // mark it as busy and return, we are in protection of mutex still now
  result->status = CacheState::BUSY;
  return result->conn;
}

// false for error, true otherwise
bool DConnCache::doReturn(DConEntry * ent){
  // assuming we are under protection of the global lock
  if (ent->status == CacheState::IDLE){
    printf("Trying to return an IDLE cache entry.\n");
    return false;
  }
  ent->status = CacheState::IDLE;
  return true;
}

// 1 for sucess, 0 for failure
bool DConnCache::ReturnDConn(duckdb::Connection* conn){
  std::lock_guard<std::mutex> guard(cap_mux);  // take a object level lock

  DConEntry * p = lookForEntryByAddr(conn);
  if (NULL != p){
    if (false == doReturn(p)){
      printf("Connection return failed.\n");
      return false;
    }
  }else{
    printf("Connection being returned doesn't exist in cache.\n");
    return false;
  }
  return true;
}

void DConnCache::Init(){
  this->current_sz = 0;
  if (NULL == dConCache){
    printf("!!! Malloc of connection entry failed.\n");
    assert(0);
  }
}

}  // end of namespace
