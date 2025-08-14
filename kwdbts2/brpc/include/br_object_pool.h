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

class ObjectPool {
 private:
  SpinLock lock_;

  using DeleteFn = void (*)(void*);
  struct Element {
    void* obj;
    DeleteFn delete_fn;
  };

  std::vector<Element> objects_;

  void DeleteAllObjects() {
    for (auto i = objects_.rbegin(); i != objects_.rend(); ++i) {
      if (i->delete_fn != nullptr) {
        i->delete_fn(i->obj);
      }
    }
  }

 public:
  void AcquireData(ObjectPool* src) {
    objects_.insert(objects_.end(), src->objects_.begin(), src->objects_.end());
    src->objects_.clear();
  }

  void Clear() {
    std::lock_guard<SpinLock> l(lock_);
    DeleteAllObjects();
    objects_.clear();
  }

  template <class T>
  T* Add(T* t) {
    std::lock_guard<SpinLock> l(lock_);
    objects_.emplace_back(Element{t, [](void* obj) { delete reinterpret_cast<T*>(obj); }});
    return t;
  }

  ObjectPool& operator=(ObjectPool&& pool) = default;
  ObjectPool(ObjectPool&& pool) = default;
  ObjectPool& operator=(const ObjectPool& pool) = delete;
  ObjectPool(const ObjectPool& pool) = delete;

  ~ObjectPool() { Clear(); }

  ObjectPool() = default;
};

}  // namespace kwdbts
