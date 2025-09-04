//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/simd_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include <xsimd/xsimd.hpp>

namespace duckdb {

// Check gcc/clang
#if defined(__GNUC__) || defined(__clang__)
// if native architecture optimisations are enabled
#if defined(__AVX2__) || defined(__AVX512F__)
#define DUCKDB_USE_VECTORIZED_CODE
#define DUCKDB_INLINE_MODE
#define DEFAULT_SIMD_BATCH_SIZE 256U
#else
#define DUCKDB_INLINE_MODE __attribute__((noinline))
#endif
#else
#define DUCKDB_INLINE_MODE 
#endif    

// Helper function: select appropriate index type based on data type
template <class T>
struct IndexTypeFor {
    // Default to uint32_t for index type
    using type = uint32_t;
};

template <>
struct IndexTypeFor<int64_t> {
    using type = uint64_t;
};

template <>
struct IndexTypeFor<uint64_t> {
    using type = uint64_t;
};

template <>
struct IndexTypeFor<float> {
    using type = uint32_t;
};

template <>
struct IndexTypeFor<double> {
    using type = uint64_t;
};

template <>
struct IndexTypeFor<int16_t> {
    using type = uint16_t;
};

template <>
struct IndexTypeFor<uint16_t> {
    using type = uint16_t;
};

template <>
struct IndexTypeFor<int8_t> {
    using type = uint8_t;
};

template <>
struct IndexTypeFor<uint8_t> {
    using type = uint8_t;
};

// Check if the type supports vectorization
template <typename TA, typename TB>
static constexpr bool IsVectorizedType() {
    return  std::is_same<TA, TB>::value &&
            (std::is_same<TA, int64_t>::value ||
            std::is_same<TA, int32_t>::value ||
            std::is_same<TA, int16_t>::value ||
            std::is_same<TA, int8_t>::value ||
            std::is_same<TA, uint64_t>::value ||
            std::is_same<TA, uint32_t>::value ||
            std::is_same<TA, uint16_t>::value ||
            std::is_same<TA, uint8_t>::value ||
            std::is_same<TA, float>::value ||
            std::is_same<TA, double>::value);
}

// Check if the operation supports vectorization
template <typename OP>
static constexpr bool IsVectorizedOp() {
    return std::is_same<OP, AddOperator>::value ||
           std::is_same<OP, SubtractOperator>::value ||
           std::is_same<OP, MultiplyOperator>::value ||
           std::is_same<OP, DivideOperator>::value;
}

#ifdef DUCKDB_USE_VECTORIZED_CODE
// TryExecuteSIMD tries to perform the SIMD calculation
template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static bool TryExecuteSIMD(const UnifiedVectorFormat &ldata, const UnifiedVectorFormat &rdata,
                                 [[maybe_unused]]RESULT_TYPE* result_data, idx_t count) {
        if constexpr (IsVectorizedType<LEFT_TYPE, RIGHT_TYPE>() && IsVectorizedOp<OP>()) {
           auto l_ptr = UnifiedVectorFormat::GetData<LEFT_TYPE>(ldata);
           auto r_ptr = UnifiedVectorFormat::GetData<RIGHT_TYPE>(rdata);
           const sel_t* l_sel = ldata.sel->data();
           const sel_t* r_sel = rdata.sel->data();
           idx_t i = 0;
           using left_index_type = typename IndexTypeFor<LEFT_TYPE>::type;
           using right_index_type = typename IndexTypeFor<RIGHT_TYPE>::type;
           using lbatch_type = xsimd::batch<LEFT_TYPE>;
           using rbatch_type = xsimd::batch<RIGHT_TYPE>;
           using index_type1 = xsimd::batch<left_index_type>;
           using index_type2 = xsimd::batch<right_index_type>;
           constexpr size_t simd_size = lbatch_type::size;
    
           lbatch_type left_vec;
           rbatch_type right_vec;
           xsimd::batch<RESULT_TYPE> result_vec;
    
           for (; i + simd_size <= count; i += simd_size) {
              left_vec = l_sel ? lbatch_type::gather(l_ptr, index_type1::load_aligned(l_sel + i)) : lbatch_type::load_aligned(l_ptr + i);
              right_vec = r_sel ? rbatch_type::gather(r_ptr, index_type2::load_aligned(r_sel + i)) : rbatch_type::load_aligned(r_ptr + i);
        
             if constexpr (std::is_same<OP, AddOperator>::value) {
                result_vec = left_vec + right_vec;
             } else if constexpr ((std::is_same<OP, SubtractOperator>::value)) {
                result_vec = left_vec - right_vec;
             } else if constexpr ((std::is_same<OP, MultiplyOperator>::value)) {
                result_vec = left_vec * right_vec;
             } else if constexpr ((std::is_same<OP, DivideOperator>::value)) {
                result_vec = left_vec / right_vec;
             }
             result_vec.store_aligned(result_data + i);
           }
           return true;
       }
    return false;
}
#endif

}