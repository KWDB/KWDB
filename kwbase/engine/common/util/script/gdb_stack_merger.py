#!/usr/bin/env python3
"""GDB Stack Trace Merger Tool - Enhanced Version
================================================
Merge similar GDB stack traces and list all related thread IDs before merged stacks.

Features:
1. Parse GDB stack trace information (supports multiple formats)
2. Normalize stack frames using regex (ignore memory addresses, parameters, etc.)
3. Merge similar stacks into one and list all thread IDs at the beginning
4. Keep dissimilar stacks separate
5. Output complete stack information to specified file

Usage:
    python3 gdb_stack_merger.py <input_file> <output_file> [options]
    
Examples:
    python3 gdb_stack_merger.py gdb_output.txt merged_output.txt
    python3 gdb_stack_merger.py gdb_output.txt merged_output.txt --verbose
"""

import re
import sys
import os
import argparse
from collections import defaultdict
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field


@dataclass
class StackFrame:
    """Represents a single stack frame"""
    frame_number: int = 0
    function_name: str = ""
    file_name: str = ""
    line_number: int = 0
    address: str = ""
    arguments: str = ""
    raw_line: str = ""
    
    def normalize(self) -> str:
        """
        Normalize stack frame, ignoring addresses and parameters.
        Only keep function name, file name, and line number.
        """
        parts = []
        if self.function_name:
            parts.append(self.function_name)
        if self.file_name:
            if self.line_number:
                parts.append(f"at {self.file_name}:{self.line_number}")
            else:
                parts.append(f"at {self.file_name}")
        
        if parts:
            return " ".join(parts)
        return self.raw_line.strip()
    
    def to_string(self) -> str:
        """Convert to string representation (for output)"""
        result = f"#{self.frame_number}  "
        if self.address:
            result += f"{self.address} in "
        result += self.function_name
        if self.arguments:
            result += f" ({self.arguments})"
        if self.file_name:
            if self.line_number:
                result += f" at {self.file_name}:{self.line_number}"
            else:
                result += f" at {self.file_name}"
        return result


@dataclass 
class StackTrace:
    """Represents a complete stack trace"""
    thread_id: str = ""
    thread_number: int = 0
    signal: str = ""
    frames: List[StackFrame] = field(default_factory=list)
    raw_text: str = ""
    
    def normalize(self) -> str:
        """
        Normalize entire stack for deduplication and grouping.
        Ignore differences in memory addresses, parameter values, etc.
        """
        normalized_frames = [frame.normalize() for frame in self.frames]
        return "\n".join(normalized_frames)
    
    def get_signature(self, depth: int = 5) -> str:
        """Get stack signature (take top N frames as features)"""
        top_frames = self.frames[:depth]
        signatures = []
        for frame in top_frames:
            if frame.function_name:
                signatures.append(frame.function_name)
            else:
                signatures.append(f"#{frame.frame_number}")
        return " -> ".join(signatures)
    
    def format_with_threads(self, thread_ids: List[str]) -> str:
        """Format stack with all related thread IDs listed at the beginning"""
        lines = []
        
        # Add thread ID list
        if thread_ids:
            thread_list = ", ".join(thread_ids)
            lines.append(f"Threads: {thread_list}")
            lines.append("")
        
        # Add signal information
        if self.signal:
            lines.append(self.signal)
            lines.append("")
        
        # Add stack frames
        for frame in self.frames:
            lines.append(frame.to_string())
        
        return "\n".join(lines)


class GDBStackParser:
    """GDB stack parser - using regex parsing"""
    
    # Regex pattern for standard GDB stack frames
    # Example: #0  0x00007f8b8c123456 in function_name (arg1=1, arg2=2) at file.cpp:123
    FRAME_PATTERN = re.compile(
        r'^#\s*(\d+)\s+'                          # Frame number
        r'(?:0x[0-9a-f]+\s+)?'                     # Optional memory address
        r'(?:in\s+)?'                              # Optional 'in' keyword
        r'([^\s(]+)'                               # Function name
        r'(?:\s*\(([^)]*)\))?'                     # Optional parameter list
        r'(?:\s+at\s+([^:]+):(\d+))?'              # Optional file name and line number
        r'\s*$'
    )
    
    # Regex pattern for simplified stack frames (KWDB format)
    # Example: #0 /path/to/library(function+offset) [0x7f8b074f396d]
    # Or: #0 /path/to/library(+0x44) [0x7f8b074f396d]
    # Or: #0 ./kwbase() [0x4fbf61]
    SIMPLE_FRAME_PATTERN = re.compile(
        r'^#\s*(\d+)\s+'                           # Frame number
        r'(.+?)'                                    # File path/library name
        r'(?:\(([^)]*)\))?'                        # Optional function name + offset
        r'\s*'
        r'(?:\[0x[0-9a-f]+\])?'                    # Optional address
        r'\s*$'
    )
    
    # Pattern for thread information (standard format)
    THREAD_PATTERN = re.compile(
        r'Thread\s+(\d+)\s*\(LWP\s+\d+\)',
        re.IGNORECASE
    )
    
    # Pattern for thread information (KWDB simplified format)
    # Example: Thread 0x7f8afd791340 pid=194618 tid=194618
    KWDB_THREAD_PATTERN = re.compile(
        r'Thread\s+(0x[0-9a-f]+)\s+pid=(\d+)\s+tid=(\d+)',
        re.IGNORECASE
    )
    
    # Pattern for signal information
    SIGNAL_PATTERN = re.compile(
        r'(Program received signal .+)',
        re.IGNORECASE
    )
    
    def __init__(self):
        self.stacks: List[StackTrace] = []
        
    def parse_file(self, filepath: str) -> List[StackTrace]:
        """Parse stacks from file"""
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        return self.parse_content(content)
    
    def parse_content(self, content: str) -> List[StackTrace]:
        """Parse GDB output content"""
        self.stacks = []
        
        # Diagnostic information
        thread_count_standard = len(re.findall(self.THREAD_PATTERN, content))
        thread_count_kwdb = len(re.findall(self.KWDB_THREAD_PATTERN, content))
        frame_count = len(re.findall(r'^#\s*\d+', content, re.MULTILINE))
        
        total_threads = thread_count_standard + thread_count_kwdb
        
        if total_threads == 0 and frame_count == 0:
            print("Warning: No thread or stack frame patterns found in input.", file=sys.stderr)
            print("Expected format examples:", file=sys.stderr)
            print("  Standard: Thread 1 (LWP 12345):", file=sys.stderr)
            print("  KWDB:     Thread 0x7f8afd791340 pid=194618 tid=194618", file=sys.stderr)
            print("  Frame:    #0  0x00007f8b in function_name at file.cpp:42", file=sys.stderr)
            print("\nPlease check your GDB output format.", file=sys.stderr)
            return []
        
        if thread_count_kwdb > 0:
            print(f"Detected KWDB format: {thread_count_kwdb} threads, {frame_count} frames")
        
        # Split multiple stacks (separated by threads)
        stack_blocks = self._split_stacks(content)
        
        for block in stack_blocks:
            stack = self._parse_single_stack(block)
            if stack and stack.frames:
                self.stacks.append(stack)
        
        return self.stacks
    
    def _split_stacks(self, content: str) -> List[str]:
        """Split content into independent stack blocks (separated by Thread)"""
        blocks = []
        current_block = []
        
        lines = content.split('\n')
        
        for line in lines:
            line_stripped = line.strip()
            # Detect new thread start (supports both formats)
            if self.THREAD_PATTERN.match(line_stripped) or self.KWDB_THREAD_PATTERN.match(line_stripped):
                if current_block:
                    blocks.append('\n'.join(current_block))
                    current_block = []
            
            current_block.append(line)
        
        # Add the last block
        if current_block:
            blocks.append('\n'.join(current_block))
        
        return blocks if blocks else [content]
    
    def _parse_single_stack(self, block: str) -> Optional[StackTrace]:
        """Parse a single stack block"""
        stack = StackTrace(raw_text=block)
        
        lines = block.split('\n')
        
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            
            # Extract thread ID and number (standard format)
            thread_match = self.THREAD_PATTERN.match(line_stripped)
            if thread_match:
                stack.thread_number = int(thread_match.group(1))
                stack.thread_id = line_stripped
                continue
            
            # Extract thread ID and number (KWDB format)
            kwdb_thread_match = self.KWDB_THREAD_PATTERN.match(line_stripped)
            if kwdb_thread_match:
                thread_addr = kwdb_thread_match.group(1)
                pid = kwdb_thread_match.group(2)
                tid = kwdb_thread_match.group(3)
                stack.thread_number = int(tid)
                stack.thread_id = f"Thread {thread_addr} pid={pid} tid={tid}"
                continue
            
            # Skip backtrace marker lines
            if line_stripped.startswith('backtrace:'):
                continue
            
            # Extract signal
            signal_match = self.SIGNAL_PATTERN.match(line_stripped)
            if signal_match:
                stack.signal = signal_match.group(1)
                continue
            
            # Parse stack frame
            frame = self._parse_frame(line_stripped)
            if frame:
                stack.frames.append(frame)
        
        # Sort by frame number
        stack.frames.sort(key=lambda f: f.frame_number)
        
        return stack if stack.frames else None
    
    def _parse_frame(self, line: str) -> Optional[StackFrame]:
        """Parse a single stack frame"""
        # First try standard format
        match = self.FRAME_PATTERN.match(line)
        if match:
            frame = StackFrame(
                frame_number=int(match.group(1)),
                function_name=match.group(2).strip(),
                arguments=match.group(3) if match.group(3) else "",
                file_name=match.group(4).strip() if match.group(4) else "",
                line_number=int(match.group(5)) if match.group(5) else 0,
                raw_line=line
            )
            # Extract address (if present)
            addr_match = re.search(r'(0x[0-9a-f]+)', line)
            if addr_match:
                frame.address = addr_match.group(1)
            return frame
        
        # Try KWDB simplified format
        simple_match = self.SIMPLE_FRAME_PATTERN.match(line)
        if simple_match:
            frame_num = int(simple_match.group(1))
            file_path = simple_match.group(2).strip()
            func_info = simple_match.group(3) if simple_match.group(3) else ""
            
            # Extract function name from file path
            function_name = ""
            file_name = file_path
            
            if func_info:
                # Format: library(function+offset) or library(+offset)
                if '+' in func_info:
                    # Extract function name (if present)
                    parts = func_info.split('+')
                    if parts[0] and parts[0] != '':
                        function_name = parts[0]
                    else:
                        # Only offset, no function name
                        function_name = os.path.basename(file_path)
                else:
                    function_name = func_info
            
            # If still no function name, use file name
            if not function_name:
                function_name = os.path.basename(file_path)
            
            # Extract address
            address = ""
            addr_match = re.search(r'\[(0x[0-9a-f]+)\]', line)
            if addr_match:
                address = addr_match.group(1)
            
            frame = StackFrame(
                frame_number=frame_num,
                function_name=function_name,
                file_name=file_name,
                line_number=0,
                address=address,
                arguments=func_info,
                raw_line=line
            )
            return frame
        
        return None


class StackMerger:
    """Stack merger - merge similar stacks and record all related threads"""
    
    def __init__(self, stacks: List[StackTrace]):
        self.stacks = stacks
        # key: normalized stack, value: (representative stack, [all thread ID list])
        self.merged_stacks: Dict[str, Tuple[StackTrace, List[str]]] = {}
        
    def merge(self) -> Dict[str, Tuple[StackTrace, List[str]]]:
        """
        Merge similar stacks.
        Returns dict: key is normalized stack, value is (representative stack object, all thread ID list)
        """
        for stack in self.stacks:
            normalized = stack.normalize()
            
            if normalized in self.merged_stacks:
                # Similar stack already exists, add current thread ID
                representative, thread_ids = self.merged_stacks[normalized]
                if stack.thread_id and stack.thread_id not in thread_ids:
                    thread_ids.append(stack.thread_id)
            else:
                # New stack pattern, create entry
                self.merged_stacks[normalized] = (stack, [stack.thread_id] if stack.thread_id else [])
        
        return self.merged_stacks
    
    def get_merged_count(self) -> int:
        """Get merged stack count"""
        return len(self.merged_stacks)
    
    def get_original_count(self) -> int:
        """Get original stack count"""
        return len(self.stacks)


class OutputGenerator:
    """Output generator - generate merged stack file"""
    
    @staticmethod
    def generate_merged_output(merger: StackMerger, sort_by_frequency: bool = True) -> str:
        """
        Generate merged stack output.
        :param merger: Stack merger
        :param sort_by_frequency: Whether to sort by frequency (more threads first)
        :return: Formatted output text
        """
        lines = []
        lines.append("=" * 80)
        lines.append("Merged GDB Stack Traces")
        lines.append("=" * 80)
        lines.append("")
        lines.append(f"Original stack count: {merger.get_original_count()}")
        lines.append(f"Merged stack count: {merger.get_merged_count()}")
        lines.append("")
        lines.append("-" * 80)
        lines.append("")
        
        # Convert to list and sort
        merged_list = list(merger.merged_stacks.items())
        
        if sort_by_frequency:
            # Sort by thread count in descending order
            merged_list.sort(key=lambda x: len(x[1][1]), reverse=True)
        
        # Output each merged stack
        for idx, (normalized, (representative_stack, thread_ids)) in enumerate(merged_list, 1):
            lines.append(f"[Stack Pattern #{idx}]")
            lines.append(f"Thread count: {len(thread_ids)}")
            lines.append("")
            
            # Format output stack with all thread IDs
            formatted_stack = representative_stack.format_with_threads(thread_ids)
            lines.append(formatted_stack)
            lines.append("")
            lines.append("-" * 80)
            lines.append("")
        
        lines.append("=" * 80)
        lines.append("End of Merged Stack Traces")
        lines.append("=" * 80)
        
        return '\n'.join(lines)


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='GDB Stack Trace Merger - Merge similar GDB stacks and list all thread IDs at the beginning',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s gdb_output.txt merged_output.txt
  %(prog)s gdb_output.txt merged_output.txt --no-sort
  %(prog)s gdb_output.txt merged_output.txt --verbose
        """
    )
    
    parser.add_argument('input_file', help='GDB output file path (input)')
    parser.add_argument('output_file', help='Merged stack file path (output)')
    parser.add_argument('--no-sort', action='store_true',
                       help='Do not sort by frequency (default: sort by thread count descending)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show detailed processing information')
    
    args = parser.parse_args()
    
    try:
        # Step 1: Parse GDB stacks
        if args.verbose:
            print(f"Parsing GDB output from: {args.input_file}")
        
        parser_obj = GDBStackParser()
        stacks = parser_obj.parse_file(args.input_file)
        
        if not stacks:
            print("Error: No stack traces found in the input file.", file=sys.stderr)
            sys.exit(1)
        
        if args.verbose:
            print(f"Found {len(stacks)} stack traces")
        
        # Step 2: Merge similar stacks
        if args.verbose:
            print("Merging similar stacks...")
        
        merger = StackMerger(stacks)
        merger.merge()
        
        if args.verbose:
            print(f"Merged into {merger.get_merged_count()} unique patterns")
        
        # Step 3: Generate merged output
        if args.verbose:
            print("Generating merged output...")
        
        sort_by_freq = not args.no_sort
        output_text = OutputGenerator.generate_merged_output(merger, sort_by_frequency=sort_by_freq)
        
        # Step 4: Write to output file
        with open(args.output_file, 'w', encoding='utf-8') as f:
            f.write(output_text)
        
        print(f"Successfully merged {merger.get_original_count()} stacks into {merger.get_merged_count()} patterns")
        print(f"Output saved to: {args.output_file}")
        
        # Print brief statistics
        print("\nSummary:")
        merged_list = list(merger.merged_stacks.items())
        if merged_list:
            merged_list.sort(key=lambda x: len(x[1][1]), reverse=True)
            for idx, (normalized, (rep_stack, thread_ids)) in enumerate(merged_list[:5], 1):
                print(f"  Pattern #{idx}: {len(thread_ids)} threads - {rep_stack.get_signature()}")
            if len(merged_list) > 5:
                print(f"  ... and {len(merged_list) - 5} more patterns")
    
    except FileNotFoundError:
        print(f"Error: File '{args.input_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
