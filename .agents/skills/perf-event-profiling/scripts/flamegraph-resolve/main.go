// flamegraph-resolve resolves [unknown] addresses in perf script output via addr2line.
//
// Usage:
//
//	perf script -i perf.data | flamegraph-resolve <dso>=<debug> [<dso>=<debug> ...]
//
// Each arg maps a DSO basename to its .debug file (build-id must match).
//
//	perf script -i perf.data | flamegraph-resolve \
//	    libkwdbts2.so=/build/lib/libkwdbts2.debug \
//	    kwbase=/build/bin/kwbase.debug \
//	| stackcollapse-perf.pl | flamegraph.pl > flame.svg
//
// How it works:
//  1. Scan stdin, collect unique addresses per .debug file
//  2. Resolve all addresses in parallel via addr2line
//  3. Re-read stdin (from temp file) and replace [unknown] with resolved names
package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type mapping struct {
	dso   string
	debug string
}

// Matches: "     deadbeef [unknown] (/path/to/dso)"
// Group 1: address hex, Group 2: DSO path
var unknownRe = regexp.MustCompile(`^\s+([0-9a-f]+)\s+\[unknown\]\s+\((.+)\)`)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <dso>=<debug> [<dso>=<debug> ...]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s kwbase=/build/bin/kwbase.debug libkwdbts2.so=/build/lib/libkwdbts2.debug\n", os.Args[0])
		os.Exit(1)
	}

	var mappings []mapping
	for _, arg := range os.Args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "Invalid mapping: %s (expected dso=debug)\n", arg)
			os.Exit(1)
		}
		mappings = append(mappings, mapping{dso: parts[0], debug: parts[1]})
		fmt.Fprintf(os.Stderr, "[flamegraph-resolve] mapped '%s' -> '%s'\n", parts[0], parts[1])
	}

	// Step 1: Scan stdin, collect unique addresses per debug file
	addresses := make(map[string]map[string]struct{}) // debug -> set of addrs
	for _, m := range mappings {
		addresses[m.debug] = make(map[string]struct{})
	}

	// Write to temp file while scanning (so we can re-read in step 3)
	tmp, err := os.CreateTemp("", "perf-script-*.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer os.Remove(tmp.Name())

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB line buffer for deep stacks
	writer := bufio.NewWriter(tmp)
	lineCount := 0
	unknownCount := 0

	for scanner.Scan() {
		line := scanner.Text()
		writer.WriteString(line)
		writer.WriteByte('\n')
		lineCount++

		if !strings.Contains(line, "[unknown]") {
			continue
		}

		m := unknownRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		addr := "0x" + m[1]
		dsoPath := m[2]
		dsoBase := filepath.Base(dsoPath)

		for _, mp := range mappings {
			if dsoBase == mp.dso || strings.HasSuffix(dsoPath, mp.dso) {
				addresses[mp.debug][addr] = struct{}{}
				unknownCount++
				break
			}
		}
	}
	writer.Flush()
	tmp.Close()

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR reading stdin: %v\n", err)
		os.Exit(1)
	}

	// Step 2: Resolve addresses in batches (100 per addr2line call) per debug file.
	// This turns 9453 exec calls into ~95 — the dominant speedup.
	resolved := make(map[string]string) // "debug:::addr" -> name
	const batchSize = 100
	var mu sync.Mutex

	var totalAddrs int
	for _, mp := range mappings {
		debug := mp.debug
		addrList := make([]string, 0, len(addresses[debug]))
		for addr := range addresses[debug] {
			addrList = append(addrList, addr)
		}
		totalAddrs += len(addrList)

		var wg sync.WaitGroup
		sem := make(chan struct{}, 10) // 10 parallel batches per debug file

		for i := 0; i < len(addrList); i += batchSize {
			end := i + batchSize
			if end > len(addrList) {
				end = len(addrList)
			}
			batch := addrList[i:end]

			wg.Add(1)
			sem <- struct{}{}
			go func(batch []string) {
				defer wg.Done()
				defer func() { <-sem }()
				results := resolveBatch(debug, batch)
				mu.Lock()
				for addr, name := range results {
					resolved[debug+":::"+addr] = name
				}
				mu.Unlock()
			}(batch)
		}
		wg.Wait()
	}

	fmt.Fprintf(os.Stderr, "[flamegraph-resolve] scanned %d lines, %d unique unknown addresses to resolve\n",
		lineCount, totalAddrs)

	// Step 3: Re-read temp file and replace [unknown] with resolved names
	f, err := os.Open(tmp.Name())
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	scanner = bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	for scanner.Scan() {
		line := scanner.Text()

		if !strings.Contains(line, "[unknown]") {
			out.WriteString(line)
			out.WriteByte('\n')
			continue
		}

		m := unknownRe.FindStringSubmatch(line)
		if m == nil {
			out.WriteString(line)
			out.WriteByte('\n')
			continue
		}

		addr := "0x" + m[1]
		dsoPath := m[2]
		dsoBase := filepath.Base(dsoPath)

		replaced := false
		for _, mp := range mappings {
			if dsoBase == mp.dso || strings.HasSuffix(dsoPath, mp.dso) {
				if name, ok := resolved[mp.debug+":::"+addr]; ok {
					line = strings.Replace(line, "[unknown]", name, 1)
					replaced = true
				}
				break
			}
		}
		if !replaced {
			// Keep hex addr as function name so it's visible in flame graph
			line = strings.Replace(line, "[unknown]", addr, 1)
		}

		out.WriteString(line)
		out.WriteByte('\n')
	}
	out.Flush()
}

func resolveBatch(debug string, addrs []string) map[string]string {
	args := make([]string, 0, len(addrs)+4)
	args = append(args, "-e", debug, "-f", "-C", "-p")
	args = append(args, addrs...)

	cmd := exec.Command("addr2line", args...)
	out, err := cmd.Output()
	if err != nil {
		return nil
	}

	// addr2line -p prints one line per address: "func at file:line"
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	result := make(map[string]string, len(addrs))

	for i, line := range lines {
		if i >= len(addrs) {
			break
		}
		addr := addrs[i]
		if line == "" || line == "??" {
			result[addr] = addr // fallback to raw address
			continue
		}
		// Strip " at file:line" suffix
		if idx := strings.Index(line, " at "); idx > 0 {
			result[addr] = line[:idx]
		} else {
			result[addr] = line
		}
	}
	return result
}
