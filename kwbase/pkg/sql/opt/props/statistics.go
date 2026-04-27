// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package props

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"github.com/olekukonko/tablewriter"
)

// Statistics is a collection of measurements and statistics that is used by
// the coster to estimate the cost of expressions. Statistics are collected
// for tables and indexes and are exposed to the optimizer via cat.Catalog
// interfaces.
//
// As logical properties are derived bottom-up for each expression, the
// estimated row count is derived bottom-up for each relational expression.
// The column statistics (stored in ColStats and MultiColStats) are derived
// lazily, and only as needed to determine the row count for the current
// expression or a parent expression. For example:
//
//	SELECT y FROM a WHERE x=1
//
// The only column that affects the row count of this query is x, since the
// distribution of values in x is what determines the selectivity of the
// predicate. As a result, column statistics will be derived for column x but
// not for column y.
//
// See memo/statistics_builder.go for more information about how statistics are
// calculated.
type Statistics struct {
	// Available indicates whether the underlying table statistics for this
	// expression were available. If true, RowCount contains a real estimate.
	// If false, RowCount does not represent reality, and should only be used
	// for relative cost comparison.
	Available bool

	// RowCount is the estimated number of rows returned by the expression.
	// Note that - especially when there are no stats available - the scaling of
	// the row counts can be unpredictable; thus, a row count of 0.001 should be
	// considered 1000 times better than a row count of 1, even though if this was
	// a true row count they would be pretty much the same thing.
	RowCount float64

	// PTagCount is the estimated number of primary tag rows returned by the expression.
	PTagCount float64

	// SortDistribution is the timestamp data distribution of primary tag in time series table.
	SortDistribution *SortDistributionStats

	// ColStats is a collection of statistics that pertain to columns in an
	// expression or table. It is keyed by a set of one or more columns over which
	// the statistic is defined.
	ColStats ColStatsMap

	// Selectivity is a value between 0 and 1 representing the estimated
	// reduction in number of rows for the top-level operator in this
	// expression.
	Selectivity float64

	// DefaultFlag
	DefaultFlag bool
}

// SortDistributionStats store sort histogram
type SortDistributionStats struct {
	SortedHistogram   *SortedHistogram
	RowCount          float64
	UnorderedRowCount float64
	Filtered          bool
}

// Init initializes the data members of Statistics.
func (s *Statistics) Init(relProps *Relational) (zeroCardinality bool) {
	if relProps.Cardinality.IsZero() {
		s.RowCount = 0
		s.Selectivity = 0
		s.Available = true
		return true
	}
	s.Selectivity = 1
	return false
}

// CopyFrom copies a Statistics object which can then be modified independently.
func (s *Statistics) CopyFrom(other *Statistics) {
	s.Available = other.Available
	s.RowCount = other.RowCount
	s.ColStats.CopyFrom(&other.ColStats)
	s.Selectivity = other.Selectivity
}

// ApplySelectivity applies a given selectivity to the statistics. RowCount and
// Selectivity are updated. Note that DistinctCounts, NullCounts, and
// Histograms are not updated.
// See ColumnStatistic.ApplySelectivity for updating distinct counts, null
// counts, and histograms.
func (s *Statistics) ApplySelectivity(selectivity float64) {
	s.RowCount *= selectivity
	s.PTagCount *= selectivity
	s.Selectivity *= selectivity
}

// UnionWith unions this Statistics object with another Statistics object. It
// updates the RowCount and Selectivity, and represents the result of unioning
// two relational expressions with the given statistics. Note that
// DistinctCounts, NullCounts, and Histograms are not updated.
func (s *Statistics) UnionWith(other *Statistics) {
	s.Available = s.Available && other.Available
	s.RowCount += other.RowCount
	s.Selectivity += other.Selectivity
}

func (s *Statistics) String() string {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "[rows=%.9g", s.RowCount)
	colStats := make(ColumnStatistics, s.ColStats.Count())
	for i := 0; i < s.ColStats.Count(); i++ {
		colStats[i] = s.ColStats.Get(i)
	}
	sort.Sort(colStats)
	for _, col := range colStats {
		fmt.Fprintf(&buf, ", distinct%s=%.9g", col.Cols.String(), col.DistinctCount)
		fmt.Fprintf(&buf, ", null%s=%.9g", col.Cols.String(), col.NullCount)
	}
	if s.SortDistribution != nil {
		fmt.Fprintf(&buf, ", filteredRowCount=%.9g", s.SortDistribution.RowCount)
		fmt.Fprintf(&buf, ", disorderRowCount=%.9g", s.SortDistribution.UnorderedRowCount)
	}
	buf.WriteString("]")
	for _, col := range colStats {
		if col.Histogram != nil {
			label := fmt.Sprintf("histogram%s=", col.Cols.String())
			indent := strings.Repeat(" ", tablewriter.DisplayWidth(label))
			fmt.Fprintf(&buf, "\n%s", label)
			histLines := strings.Split(strings.TrimRight(col.Histogram.String(), "\n"), "\n")
			for i, line := range histLines {
				if i != 0 {
					fmt.Fprintf(&buf, "\n%s", indent)
				}
				fmt.Fprintf(&buf, "%s", strings.TrimRight(line, " "))
			}
		}
	}

	return buf.String()
}

// ColumnStatistic is a collection of statistics that applies to a particular
// set of columns. In theory, a table could have a ColumnStatistic object
// for every possible subset of columns. In practice, it is only worth
// maintaining statistics on a few columns and column sets that are frequently
// used in predicates, group by columns, etc.
//
// ColumnStatistiscs can be copied by value.
type ColumnStatistic struct {
	// Cols is the set of columns whose data are summarized by this
	// ColumnStatistic struct. The ColSet is never modified in-place.
	Cols opt.ColSet

	// DistinctCount is the estimated number of distinct values of this
	// set of columns for this expression. Includes null values.
	DistinctCount float64

	// NullCount is the estimated number of null values of this set of
	// columns for this expression. For multi-column stats, this null
	// count tracks all instances of at least one null value in the
	// column set.
	NullCount float64

	// Histogram is only used when the size of Cols is one. It contains
	// the approximate distribution of values for that column, represented
	// by a slice of histogram buckets.
	Histogram *Histogram
}

// ApplySelectivity updates the distinct count, null count, and histogram
// according to a given selectivity.
func (c *ColumnStatistic) ApplySelectivity(selectivity, inputRows float64) {
	// Since the null count is a simple count of all null rows, we can
	// just multiply the selectivity with it.
	c.NullCount *= selectivity

	if c.Histogram != nil {
		c.Histogram = c.Histogram.ApplySelectivity(selectivity)
	}

	if selectivity == 1 || c.DistinctCount == 0 {
		return
	}
	if selectivity == 0 {
		c.DistinctCount = 0
		return
	}

	n := inputRows
	d := c.DistinctCount

	// If each distinct value appears n/d times, and the probability of a
	// row being filtered out is (1 - selectivity), the probability that all
	// n/d rows are filtered out is (1 - selectivity)^(n/d). So the expected
	// number of values that are filtered out is d*(1 - selectivity)^(n/d).
	//
	// This formula returns d * selectivity when d=n but is closer to d
	// when d << n.
	c.DistinctCount = d - d*math.Pow(1-selectivity, n/d)
	const epsilon = 1e-10
	if c.DistinctCount < epsilon {
		// Avoid setting the distinct count to 0 (since the row count is
		// non-zero).
		c.DistinctCount = epsilon
	}

}

// ColumnStatistics is a slice of pointers to ColumnStatistic values.
type ColumnStatistics []*ColumnStatistic

// Len returns the number of ColumnStatistic values.
func (c ColumnStatistics) Len() int { return len(c) }

// Less is part of the Sorter interface.
func (c ColumnStatistics) Less(i, j int) bool {
	if c[i].Cols.Len() != c[j].Cols.Len() {
		return c[i].Cols.Len() < c[j].Cols.Len()
	}

	prev := opt.ColumnID(0)
	for {
		nextI, ok := c[i].Cols.Next(prev)
		if !ok {
			return false
		}

		// No need to check if ok since both ColSets are the same length and
		// so far have had the same elements.
		nextJ, _ := c[j].Cols.Next(prev)

		if nextI != nextJ {
			return nextI < nextJ
		}

		prev = nextI
	}
}

// Swap is part of the Sorter interface.
func (c ColumnStatistics) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
