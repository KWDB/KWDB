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

package mysqlwire

// setCharsetResultsNoop replaces the NULL, and is what makes the statement an
// assignment of the variable's own value to itself.
//
// DEFAULT would parse too, and today the two cannot be told apart:
// character_set_results is a compat variable whose getter returns a constant
// captured at registration, so nothing can move it. That is not a property to
// build on. If the variable ever becomes stateful, DEFAULT would silently
// discard a value the client set earlier, while reading and writing the same
// value stays an exact no-op whatever the value is.
//
// The one-argument form of current_setting is deliberate here, against the
// two-argument form every expression in sysvars.go uses. Those answer a client
// asking to read nineteen variables at once, where one unknown name must not
// refuse the whole statement. This one is a write with a single name in it: if
// the variable is ever removed, an accurate "unrecognized configuration
// parameter" naming it is a better outcome than feeding NULL back into the SET
// and getting an error about types.
const setCharsetResultsNoop = `current_setting('character_set_results')`

// rewriteSetCharsetResultsNull turns MySQL's `SET character_set_results = NULL`
// into a statement that does nothing, which is what the MySQL statement asks
// for.
//
// In MySQL that assignment means "do not transcode the result set on the
// server". KWDB stores and emits UTF-8 and never transcodes, so there is no
// conversion left to switch off and the request is met by doing nothing -- it
// is honored, not pretended. Connector/J sends it during the connection phase
// after being told character_set_results is utf8mb4, and KWDB's answer today,
// 1105 `parameter "character_set_results" requires a string value`, is a
// connection that never comes up rather than a feature that degrades.
//
// Accepting NULL in the SQL layer instead would reach much further than the one
// statement that needs it: the rejection lives in the generic value conversion
// every SET goes through, so lifting it would make `SET search_path = NULL`
// legal too.
//
// Nothing that works today changes meaning. character_set_results accepts only
// utf8, utf8mb3, utf8mb4 and DEFAULT, so the input recognized here is disjoint
// from the input that succeeds today.
//
// Recognition is prefix-shaped and all-or-nothing, per the contract in
// rewriteMySQLDialect: anything not read to the end of the statement is
// returned unchanged for the SQL layer to reject as before.
func rewriteSetCharsetResultsNull(query string) string {
	i := skipLeadingTrivia(query, 0)
	if !matchKeyword(query, &i, "SET") {
		return query
	}
	i = skipSpaces(query, i)
	// SESSION and LOCAL both name the only scope KWDB has, so a client may
	// state either or leave it out. GLOBAL is not accepted: MySQL's global
	// scope is a second value space KWDB does not have, and answering a write
	// to it with a session no-op would report success for something that did
	// not happen.
	if matchKeyword(query, &i, "SESSION") || matchKeyword(query, &i, "LOCAL") {
		i = skipSpaces(query, i)
	}

	// matchKeyword is what reads the variable name, rather than setnames.go's
	// consumeKeyword: that one requires the keyword to end at an isSeparator,
	// and here the very next byte is an `=`. Widening the shared isSeparator to
	// admit it would change how SET NAMES and its COLLATE clause are scanned,
	// which is a rule this statement has no business touching.
	if !matchKeyword(query, &i, "character_set_results") {
		return query
	}
	i = skipSpaces(query, i)
	if byteAt(query, i) == '=' {
		i++
	} else if !matchKeyword(query, &i, "TO") {
		return query
	}
	i = skipSpaces(query, i)

	// The value has to be exactly NULL, and the statement has to end there.
	// matchKeyword rejects a longer identifier on its own -- NULLIF is a
	// different word, and `SET character_set_results = NULLIF('a','b')` is a
	// statement KWDB evaluates, reaching the variable with the value 'a' -- and
	// the terminator check rejects a NULL that merely begins a larger
	// expression or a longer mode list.
	nullStart := i
	if !matchKeyword(query, &i, "NULL") {
		return query
	}
	nullEnd := i
	if !atStatementEnd(query, nullEnd) {
		return query
	}

	// Rule R1: only the four bytes of NULL are replaced, so a later error
	// message still quotes the statement the client sent.
	return query[:nullStart] + setCharsetResultsNoop + query[nullEnd:]
}

// atStatementEnd reports whether nothing but the end of the statement follows
// i: whitespace, and then either the end of the input or a semicolon.
//
// This is the terminator both rewrites in this file require, and it is stricter
// than asking whether the next byte separates tokens. Every statement they
// recognize can legally carry more after the part they read -- another
// assignment, another transaction mode -- and a shape treated as complete while
// the client wrote more would be a rewrite that silently dropped half of a
// statement.
func atStatementEnd(s string, i int) bool {
	i = skipSpaces(s, i)
	return i == len(s) || s[i] == ';'
}

// matchIsolationLevel matches one of the isolation levels KWDB has at *i and,
// on success, advances *i past it.
//
// READ UNCOMMITTED is deliberately absent: KWDB has no such level, so the
// parser refuses the statement by naming the word it cannot take, which tells
// the client exactly what is unavailable. Mapping it onto READ COMMITTED would
// answer a request for one level with another and report success.
func matchIsolationLevel(s string, i *int) bool {
	j := *i
	switch {
	case matchKeyword(s, &j, "SERIALIZABLE"):
	case matchKeyword(s, &j, "REPEATABLE"):
		j = skipSpaces(s, j)
		if !matchKeyword(s, &j, "READ") {
			return false
		}
	case matchKeyword(s, &j, "READ"):
		j = skipSpaces(s, j)
		if !matchKeyword(s, &j, "COMMITTED") {
			return false
		}
	default:
		return false
	}
	*i = j
	return true
}

// rewriteSetSessionTransaction turns MySQL's SET SESSION TRANSACTION into the
// KWDB statement that asks for the same thing.
//
// In MySQL the statement sets the default for transactions started later in
// this session. KWDB spells that SET SESSION CHARACTERISTICS AS TRANSACTION,
// and reads MySQL's spelling as a statement about the transaction currently
// running -- so a connection pool, which sends it from
// Connection.setTransactionIsolation() while no transaction is running, is told
// "there is no transaction in progress": an error about something it never
// mentioned. All three levels the statement can name exist in KWDB and are
// carried down to the KV layer, so this is a request KWDB can meet rather than
// one it has to refuse.
//
// This is the one rewrite in the package that changes what an already valid
// statement does. Inside an explicit transaction the MySQL spelling used to
// change the running transaction; it now sets the session default, and the
// running transaction stays on the level it began with until it commits. The
// rewrite cannot be made conditional on being outside a transaction: it runs
// before the statement is queued, and the transaction state it would have to
// consult belongs to the executor goroutine.
//
// Exactly one shape is recognized, and the statement has to end right after it.
// SET SESSION TRANSACTION and SET SESSION CHARACTERISTICS AS TRANSACTION share
// the grammar's transaction_mode_list, so any mode list parses on both sides
// and a wider match would still produce valid SQL. Their execution paths differ
// on three points, and each one loses something the client asked for:
// set_default_isolation.go refuses a PRIORITY with 0A000 and leaves the
// transaction aborted, never reads AS OF SYSTEM TIME at all -- a historical
// read becomes a current one with nothing in the result to say so -- and forces
// SERIALIZABLE whenever no ISOLATION clause is present.
//
// That last point is why READ ONLY and READ WRITE become the session variable
// instead: written as CHARACTERISTICS they would parse, and would reset the
// isolation level a pool had just set, since setReadOnly() is the call that
// follows setTransactionIsolation().
func rewriteSetSessionTransaction(query string) string {
	i := skipLeadingTrivia(query, 0)
	if !matchKeyword(query, &i, "SET") {
		return query
	}
	i = skipSpaces(query, i)

	// SESSION is required, and is the whole reason this statement is about the
	// session default. Without it the statement is about the running
	// transaction, which is how KWDB already reads it, and GLOBAL names a
	// second value space KWDB does not have.
	scopeStart := i
	if !matchKeyword(query, &i, "SESSION") {
		return query
	}
	scopeEnd := i
	i = skipSpaces(query, i)
	if !matchKeyword(query, &i, "TRANSACTION") {
		return query
	}
	i = skipSpaces(query, i)

	switch {
	case matchKeyword(query, &i, "ISOLATION"):
		i = skipSpaces(query, i)
		if !matchKeyword(query, &i, "LEVEL") {
			return query
		}
		i = skipSpaces(query, i)
		if !matchIsolationLevel(query, &i) || !atStatementEnd(query, i) {
			return query
		}
		// Rule R1: two words are inserted and nothing else moves, so a later
		// error message still quotes the statement the client sent.
		return query[:scopeEnd] + " CHARACTERISTICS AS" + query[scopeEnd:]

	case matchKeyword(query, &i, "READ"):
		var value string
		i = skipSpaces(query, i)
		switch {
		case matchKeyword(query, &i, "ONLY"):
			value = "true"
		case matchKeyword(query, &i, "WRITE"):
			value = "false"
		default:
			return query
		}
		if !atStatementEnd(query, i) {
			return query
		}
		return query[:scopeStart] + "default_transaction_read_only = " + value + query[i:]
	}
	return query
}
