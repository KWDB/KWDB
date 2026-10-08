// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#ifndef COMMON_SRC_H_TR_API_H_
#define COMMON_SRC_H_TR_API_H_

#include "cm_module.h"
#include "tr_impl.h"

namespace kwdbts {
/*!
 * staged out for now
 */
#define TRACE(ctx, level, fmt, ...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, level, fmt, ##__VA_ARGS__)

#define TRACE_LEVEL4(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 4, __VA_ARGS__)
#define TRACE_LEVEL3(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 3, __VA_ARGS__)
#define TRACE_LEVEL2(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 2, __VA_ARGS__)
#define TRACE_LEVEL1(...) kwdbts::TRACER.Trace(ctx, __FILE__, __LINE__, 1, __VA_ARGS__)

}  // namespace kwdbts

#endif  // COMMON_SRC_H_TR_API_H_
