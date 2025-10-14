// Copyright 2017 The Cockroach Authors.
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

package treeprinter

import (
	"bytes"
	"fmt"
	"strings"
)

var (
	edgeLinkChr = rune('│')
	edgeMidChr  = rune('├')
	edgeLastChr = rune('└')
)

// Node is a handle associated with a specific depth in a tree. See below for
// sample usage.
type Node struct {
	tree  *tree
	level int
}

// New creates a tree printer and returns a sentinel node reference which
// should be used to add the root. Sample usage:
//
//	tp := New()
//	root := n.Child("root")
//	root.Child("child-1")
//	root.Child("child-2").Child("grandchild\ngrandchild-more-info")
//	root.Child("child-3")
//
//	fmt.Print(tp.String())
//
// Output:
//
//	root
//	 ├── child-1
//	 ├── child-2
//	 │    └── grandchild
//	 │        grandchild-more-info
//	 └── child-3
//
// Note that the Child calls can't be rearranged arbitrarily; they have
// to be in the order they need to be displayed (depth-first pre-order).
func New() Node {
	return NewWithIndent(true, true, 2)
}

// NewWithIndent creates a tree printer like New, permitting customization of
// the width of the outputted tree.
// leftPad controls whether the tree lines are padded from the first character
// of their parent.
// rightPad controls whether children are separated from their edges by a space.
// edgeLength controls how many characters wide each edge is.
func NewWithIndent(leftPad, rightPad bool, edgeLength int) Node {
	var leftPadStr, rightPadStr string
	var edgeBuilder strings.Builder
	for i := 0; i < edgeLength; i++ {
		edgeBuilder.WriteRune('─')
	}
	edgeStr := edgeBuilder.String()
	if leftPad {
		leftPadStr = " "
	}
	if rightPad {
		rightPadStr = " "
	}
	edgeLink := fmt.Sprintf("%s%c", leftPadStr, edgeLinkChr)
	edgeMid := fmt.Sprintf("%s%c%s%s", leftPadStr, edgeMidChr, edgeStr, rightPadStr)
	edgeLast := fmt.Sprintf("%s%c%s%s", leftPadStr, edgeLastChr, edgeStr, rightPadStr)
	return Node{
		tree: &tree{
			edgeLink: []rune(edgeLink),
			edgeMid:  []rune(edgeMid),
			edgeLast: []rune(edgeLast),
		},
		level: 0,
	}
}

type tree struct {
	// rows maintains the rows accumulated so far, as rune arrays.
	//
	// When a new child is added (e.g. child2 above), we may have to
	// go back up and fix edges.
	rows [][]rune

	// row index of the last row for a given level. Grows as needed.
	lastNode []int

	edgeLink []rune
	edgeMid  []rune
	edgeLast []rune
}

// Childf adds a node as a child of the given node.
func (n Node) Childf(format string, args ...interface{}) Node {
	return n.Child(fmt.Sprintf(format, args...))
}

// Child adds a node as a child of the given node. Multi-line strings are
// supported with appropriate indentation.
func (n Node) Child(text string) Node {
	if strings.ContainsRune(text, '\n') {
		splitLines := strings.Split(text, "\n")
		node := n.childLine(splitLines[0])
		for _, l := range splitLines[1:] {
			n.AddLine(l)
		}
		return node
	}
	return n.childLine(text)
}

// AddLine adds a new line to a child node without an edge.
func (n Node) AddLine(v string) {
	// Each level indents by this much.
	k := len(n.tree.edgeLast)
	indent := n.level * k
	row := make([]rune, indent+len(v))
	for i := 0; i < indent; i++ {
		row[i] = ' '
	}
	for i, r := range v {
		row[indent+i] = r
	}
	n.tree.rows = append(n.tree.rows, row)
}

// childLine adds a node as a child of the given node.
func (n Node) childLine(text string) Node {
	runes := []rune(text)

	// Each level indents by this much.
	k := len(n.tree.edgeLast)
	indent := n.level * k
	row := make([]rune, indent+len(runes))
	for i := 0; i < indent-k; i++ {
		row[i] = ' '
	}
	if indent >= k {
		// Connect through any empty lines.
		for i := len(n.tree.rows) - 1; i >= 0 && len(n.tree.rows[i]) == 0; i-- {
			n.tree.rows[i] = make([]rune, indent-k+len(n.tree.edgeLink))
			for j := 0; j < indent-k+len(n.tree.edgeLink); j++ {
				n.tree.rows[i][j] = ' '
			}
			copy(n.tree.rows[i][indent-k:], n.tree.edgeLink)
		}
		copy(row[indent-k:], n.tree.edgeLast)
	}
	copy(row[indent:], runes)

	for len(n.tree.lastNode) <= n.level+1 {
		n.tree.lastNode = append(n.tree.lastNode, -1)
	}
	n.tree.lastNode[n.level+1] = -1

	if last := n.tree.lastNode[n.level]; last != -1 {
		if n.level == 0 {
			panic("multiple root nodes")
		}
		// Connect to the previous sibling.
		copy(n.tree.rows[last][indent-k:], n.tree.edgeMid)
		for i := last + 1; i < len(n.tree.rows); i++ {
			// Add spaces if necessary.
			for len(n.tree.rows[i]) < indent-k+len(n.tree.edgeLink) {
				n.tree.rows[i] = append(n.tree.rows[i], ' ')
			}
			copy(n.tree.rows[i][indent-k:], n.tree.edgeLink)
		}
	}

	n.tree.lastNode[n.level] = len(n.tree.rows)
	n.tree.rows = append(n.tree.rows, row)

	// Return a TreePrinter that can be used for children of this node.
	return Node{
		tree:  n.tree,
		level: n.level + 1,
	}
}

// AddEmptyLine adds an empty line to the output; used to introduce vertical
// spacing as needed.
func (n Node) AddEmptyLine() {
	n.tree.rows = append(n.tree.rows, []rune{})
}

// FormattedRows returns the formatted rows. Can only be called on the result of
// treeprinter.New.
func (n Node) FormattedRows() []string {
	if n.level != 0 {
		panic("Only the root can be stringified")
	}
	res := make([]string, len(n.tree.rows))
	for i, r := range n.tree.rows {
		res[i] = string(r)
	}
	return res
}

func (n Node) String() string {
	if n.level != 0 {
		panic("Only the root can be stringified")
	}
	var buf bytes.Buffer
	for _, r := range n.tree.rows {
		buf.WriteString(string(r))
		buf.WriteByte('\n')
	}
	return buf.String()
}
