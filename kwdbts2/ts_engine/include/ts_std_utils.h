#pragma once
#include <cstdint>
#include <iterator>

namespace kwdbts {

class IndexRange {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = int64_t;
  using difference_type = int64_t;
  using pointer = int64_t *;
  using reference = int64_t &;

 private:
  int64_t index = 0;

 public:
  IndexRange() = default;
  explicit IndexRange(int64_t index) : index(index) {}
  operator int64_t() const { return index; }

  // named requirements: LegacyIterator
  int64_t operator*() const { return index; }
  IndexRange &operator++() {
    ++index;
    return *this;
  }

  //   named requirements: LegacyInputIterator
  bool operator!=(const IndexRange &other) const { return index != other.index; }
  IndexRange operator++(int) {
    IndexRange tmp = *this;
    ++tmp;
    return tmp;
  }

  //   named requirements: LegacyForwardIterator
  bool operator==(const IndexRange &other) const { return index == other.index; }

  //   named requirements: LegacyBidirectionalIterator
  IndexRange &operator--() {
    --index;
    return *this;
  }
  IndexRange operator--(int) {
    IndexRange tmp = *this;
    --tmp;
    return tmp;
  }

  bool operator<(const IndexRange &other) const { return index < other.index; }
  bool operator<=(const IndexRange &other) const { return index <= other.index; }
  bool operator>(const IndexRange &other) const { return index > other.index; }
  bool operator>=(const IndexRange &other) const { return index >= other.index; }

  difference_type operator-(const IndexRange &rhs) const { return index - rhs.index; }

  IndexRange &operator+=(difference_type n) {
    index += n;
    return *this;
  }
  IndexRange &operator-=(difference_type n) {
    index -= n;
    return *this;
  }

  IndexRange operator+(difference_type n) const {
    IndexRange tmp = *this;
    tmp.index += n;
    return tmp;
  }
  IndexRange operator-(difference_type n) const {
    IndexRange tmp = *this;
    tmp.index -= n;
    return tmp;
  }

  value_type operator[](difference_type n) const { return index + n; }

  friend IndexRange operator+(IndexRange::difference_type n, const IndexRange &it) {
    IndexRange tmp = it;
    tmp.index += n;
    return tmp;
  }
};
}  // namespace kwdbts
