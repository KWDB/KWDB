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


#ifndef KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_TS_HYPERLOGLOG_H_
#define KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_TS_HYPERLOGLOG_H_
#include <iostream>
#include <algorithm>
#include <vector>
#include <cmath>
#include <unordered_set>
#include <memory>
#include <utility>
#include "ts_utils.h"

namespace kwdbts {
// Calculate distinct-count maximum column width
// 4 byte Markers + (CompressedList length + 8 byte Markers) + (tmpSet length + 4 byte Markers)
#define MAX_SKETCH_LEN (4 + 8192+8 + 81*4+4)

const byte capacity = 16;
const byte pp = 25;
const uint32_t mp = 1 << pp;
const byte version = 1;

// Store encoded hash value in sparse mode
using Set = std::unordered_set<uint32_t>;
// Stores variable-length encoded hash value
using VariableLengthList = std::vector<byte>;

using reg = byte;
using tailcuts = std::vector<reg>;
class Iterator;

// Counter under dense representation
class Registers {
 public:
    explicit Registers(uint32_t size) : tailcuts_(size / 2), nz_(size) {}

    uint8_t min() const;

   /*
    * @Description : Retrieves the value of a specific register from the Registers array. Each byte in the array holds two register values,
    *                packed as high and low 4-bit values. This method determines whether to extract the high or low 4-bit value based on the index provided.
    * @IN i : The index of the register to retrieve. This index is zero-based and refers to one of the two 4-bit registers stored in each byte.
    * @OUT : None. This method does not modify any state.
    * @Return : The 4-bit value stored in the specified register. The return type is an 8-bit unsigned integer, but only the lower 4 bits contain the register's value.
    */
    uint8_t get(uint32_t i) const;

   /*
    * @Description : Sets the value of a specific register within the Registers array. Each byte in the array contains two register values,
    *                packed as high and low 4-bit values. This method manages the bit-packing and correctly updates the appropriate half of the byte.
    *                It also tracks the number of non-zero values in the registers, adjusting this count based on the change from and to zero values.
    * @IN i : The index of the register to be set. This index is zero-based and each index refers to one of the two 4-bit registers in a byte.
    * @IN val : The value to set in the register. This value should be a 4-bit number (i.e., between 0 and 15).
    * @OUT tailcuts_ : The array where register values are stored. The specified register is updated within this array.
    * @OUT nz_ : The count of non-zero registers. This is incremented or decremented based on whether the operation
    *            changes a register from zero to non-zero, or vice versa.
    */
    void set(uint32_t i, uint8_t val);

   /*
    * @Description : Reduces the value of each register in the array by a specified delta, adjusting each value without underflowing below zero.
    *                This operation is used to normalize the values after some significant change to the dataset or the sketch's parameters.
    *                The method also recalculates the count of non-zero registers after the adjustment.
    * @IN delta : The amount by which to reduce each register's value. This is subtracted from each stored 4-bit value in the array.
    * @OUT tailcuts_ : The array where register values are stored. Each register's value is reduced by the delta, adjusted within bounds.
    * @OUT nz_ : The count of non-zero registers is recalculated to reflect changes made during the rebasing.
    */
    void rebase(uint8_t delta);

    tailcuts tailcuts_;
    uint32_t nz_;
};

// Used to merge sparse
class CompressedList {
 public:
  explicit CompressedList(size_t size) : count_(0), last_(0) {
    // Reserve space
    b_.reserve(size);
  }

 /*
  * @Description : Appends a new value to the compressed list by encoding the difference between the given value and the last value stored.
  *                The difference is encoded using variable-length encoding, optimizing storage for smaller differences.
  * @IN x : The new value to be appended to the list.
  * @OUT count_ : Incremented to reflect the addition of a new value to the list.
  * @OUT last_ : Updated to the new value x after appending it to the list.
  * @OUT b_ : The byte vector that stores the compressed data. The difference is encoded into this vector.
  */
  void Append(uint32_t x);

 /*
  * @Description : Serializes the compressed list into a binary format suitable for storage or transmission.
  *                This method encodes the total count of elements, the last value appended, and the compressed data bytes.
  *                The serialization includes first encoding the count and last value in a fixed-width binary format,
  *                followed by the already encoded bytes of the list.
  * @OUT bdata : The vector of bytes that holds the serialized form of the compressed data.
  * @OUT data : The final vector of bytes that combines all parts of the compressed list's state into a binary format.
  * @Return : A vector of bytes containing the serialized state of the compressed list.
  */
  std::vector<byte> MarshalBinary() const;

  // Get length
  size_t len() const;

 /*
  * @Description : Decodes a value from a compressed list that uses variable-length encoding. This method starts decoding at a given index and
  *                reconstructs the original value by combining it with the last known value.
  *                It handles the continuation bits marked by the 0x80 mask and accumulates the value until it reaches a byte without this mask,
  *                indicating the end of the encoded integer.
  * @IN i : The starting index in the byte vector from which the decoding begins.
  * @IN last : The last known value, used to reconstruct the original value by adding the decoded difference.
  * @OUT : Updates the local variables within the function but does not alter any external state.
  * @Return : A pair consisting of the decoded integer and the new index position right after the end of the decoded value.
  */
  std::pair<uint32_t, int> decode(int i, uint32_t last) const;

  // Get Iterator
  std::unique_ptr<Iterator> Iter() const;

 private:
  uint32_t count_;
  uint32_t last_;
  VariableLengthList b_;
};

// Iterator is used to traverse compressedList
class Iterator {
 public:
  explicit Iterator(const CompressedList* list) : index(0), lastValue(0), list(list) {}

 /*
  * @Description : Retrieves the next value from the list being iterated over by decoding the current index.
  *                This method updates the iterator's position and the last accessed value.
  *                The decoding is based on a specific format stored in the list which likely includes compressed data.
  * @OUT lastValue : Stores the last value retrieved by the iterator, updated with each call to Next.
  * @OUT index : The current index in the list being iterated, updated to point to the next item after retrieving the current item.
  * @Return : The next decoded value from the list.
  */
  uint32_t Next();

  // Get current value in CompressedList
  uint32_t Peek() const;

  bool HasNext() const;

 private:
  int index;
  uint32_t lastValue;
  const CompressedList* list;
};

// Sketch is a HyperLogLog data-structure for the count-distinct problem,
// approximating the number of distinct elements in a multiset.
class Sketch {
 public:
   /*
    * @Description : Initializes a HyperLogLog sketch with a given precision. This sketch can operate in sparse mode or dense mode.
    *                In sparse mode, it uses a compressed list to store the data. In dense mode, it uses registers.
    *                The precision controls the number of buckets used in the sketch, influencing its accuracy and memory usage.
    * @IN precision : The precision level of the HyperLogLog sketch. It determines the exponential size of the sketch.
    *                 Valid range for precision is typically between 4 and 18.
    * @IN sparse : Determines whether the sketch will operate in sparse mode (true) or dense mode (false).
    * @OUT : Constructs an instance of Sketch with initialized parameters for either sparse or dense mode.
    */
    Sketch(byte precision, bool sparse);

    // Add element e to Sketch
    void Insert(const std::vector<byte>& e);

   /*
    * @Description : Updates the value in a specific index of the registers within a HyperLogLog sketch, considering the rank of the item.
    *                If the incoming rank significantly exceeds the current base (b_), the registers are rebased to normalize the data.
    *                This function is crucial for maintaining the accuracy of the HyperLogLog estimates.
    * @IN index : The index in the register array where the update needs to be applied.
    * @IN rank : The rank of the item being inserted, which is used to update the register if it exceeds the current base (b_).
    * @OUT regs_ : The register array that might be updated with a new value at the specified index. This array reflects the frequencies of hashed values.
    */
    void insert(uint32_t index, byte rank);

   /*
    * @Description : Inserts a hashed value into the HyperLogLog sketch. It processes the hash according to the current mode of the sketch (sparse or normal).
    *                In sparse mode, hashes are encoded and added to a temporary set until a threshold is reached, prompting a merge and potential mode switch.
    *                In normal mode, the hash is directly processed into the registers.
    * @IN hash : The hash value to be inserted, derived from the original data.
    * @OUT : Depending on the mode, updates either a temporary set or the registers directly.
    */
    void InsertHash(uint64_t hash);

   /*
    * @Description : Serializes the current state of the HyperLogLog sketch into a binary format.
    *                This method prepares the sketch for storage or transmission by encoding its configuration and data.
    *                It handles both sparse and dense modes differently, ensuring all relevant data is included in the serialization.
    * @OUT data : A vector of bytes that represents the serialized state of the HyperLogLog sketch.
    *             This includes configuration settings and the data contents of either the sparse set and list or the dense registers.
    * @Return : A vector of bytes containing the serialized state of the sketch.
    */
    std::vector<byte> MarshalBinary() const;

    uint8_t precision() const { return p_;}

    bool isSparse() const {return sparse_;}

    Set GetTmpSet() const {return tmpSet_;}

    std::shared_ptr<Registers> GetRegs() const {return regs_;}

    std::shared_ptr<CompressedList> GetSparseList() const {return sparseList_;}

   /*
    * @Description : Merges the temporary set of sparse entries with the existing compressed list to form a new compressed list.
    *                This method is called when the temporary set becomes too large or when transitioning from sparse to normal mode.
    *                It sorts the entries in the temporary set and then merges them with the entries in the existing compressed list, maintaining sorted order.
    * @OUT sparseList_ : The existing compressed list is replaced with a newly created list that merges both the temporary set and the old list's data.
    * @OUT tmpSet_ : Cleared after merging its contents into the new compressed list.
    */
    void mergeSparse();

   /*
    * @Description : Transitions the HyperLogLog sketch from sparse mode to normal (dense) mode. This method first merges any remaining sparse data,
    *                initializes a new Registers object for normal mode operation, and then moves data from a sparse list into these registers.
    *                This conversion is critical when the sparse representation is no longer efficient due to increased data size.
    * @OUT regs_ : A new Registers object is created and initialized based on the current sketch size (m_).
    * @OUT sparse_ : Updated to false to indicate that the sketch is no longer in sparse mode.
    * @OUT tmpSet_ : Cleared to remove any temporary sparse entries.
    * @OUT sparseList_ : Set to nullptr, effectively clearing the list as the sketch transitions away from sparse mode.
    */
    void toNormal();

 private:
    // Whether to use sparse representation
    bool sparse_;
    // Precision
    uint8_t p_;
    // Estimated number of bytes of memory occupied
    uint8_t b_;
    // Number of buckets
    uint32_t m_;
    // Used for deviation correction
    double alpha_;
    Set tmpSet_;
    // Arrays that can be stored in a sparse representation
    std::shared_ptr<CompressedList> sparseList_;
    // Register
    std::shared_ptr<Registers> regs_;
};

}  // namespace kwdbts

#endif  // KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_TS_HYPERLOGLOG_H_
