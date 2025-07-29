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

#include <mutex>
#include <vector>

#include "br_spinlock.h"

namespace kwdbts {

// An ObjectPool maintains a list of C++ objects which are deallocated
// by destroying the pool.
// Thread-safe.
class ObjectPool {
 public:
  ObjectPool() = default;

  ~ObjectPool() { Clear(); }

  ObjectPool(const ObjectPool& pool) = delete;
  ObjectPool& operator=(const ObjectPool& pool) = delete;
  ObjectPool(ObjectPool&& pool) = default;
  ObjectPool& operator=(ObjectPool&& pool) = default;

  template <class T>
  T* Add(T* t) {
    std::lock_guard<SpinLock> l(lock_);
    objects_.emplace_back(Element{t, [](void* obj) { delete reinterpret_cast<T*>(obj); }});
    return t;
  }

  void Clear() {
    std::lock_guard<SpinLock> l(lock_);
    for (auto i = objects_.rbegin(); i != objects_.rend(); ++i) {
      if (i->delete_fn != nullptr) {
        i->delete_fn(i->obj);
      }
    }
    objects_.clear();
  }

  void AcquireData(ObjectPool* src) {
    objects_.insert(objects_.end(), src->objects_.begin(), src->objects_.end());
    src->objects_.clear();
  }

 private:
  // A generic deletion function pointer. Deletes its first argument.
  using DeleteFn = void (*)(void*);

  // For each object, a pointer to the object and a function that deletes it.
  struct Element {
    void* obj;
    DeleteFn delete_fn;
  };

  std::vector<Element> objects_;
  SpinLock lock_;
};

}  // namespace kwdbts
