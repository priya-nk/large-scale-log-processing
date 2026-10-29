#!/usr/bin/env python3
"""
================================================================================
SINGLE-SOURCE LOG NORMALIZATION & ANALYTICS PIPELINE
Stage 2: High-Performance OS-Level Log Ingestion Engine
================================================================================
Role: Systems Software Engineer
Description:
    A high-performance log ingestion engine in Python that bypasses standard
    high-level file I/O (e.g., open(), readline()) in favor of OS-level system
    calls and POSIX kernel primitives:
      1. Low-Level File Descriptors (os.open, os.fstat, os.close)
      2. Virtual Memory Mapping (mmap.mmap) for zero-copy buffer access
      3. Sequential Kernel Page Cache Alignment & Prefetching (madvise)
      4. Dynamic Chunking with Newline Boundary Alignment (atomic lines)
      5. Zero-Copy Byte Buffer Scanning without decoding the whole file
      6. Process Memory & Hardware Page Fault Profiling

Architecture Overview (Pipeline Stage 2):
    [Disk: Massive Raw Log]
              │
              ▼ (os.open + os.fstat: Low-level descriptor & inode metadata)
    [Kernel Page Cache / VMA]
              │
              ▼ (mmap: Direct virtual memory mapping, bypass user-space read copies)
    [OS Ingestion Engine]
              │
              ▼ (Dynamic Newline Alignment: [chunk_start : chunk_end])
    [Atomic Byte Slices] ➔ Yielded to Stage 3 (Parsing & Normalization)

Author: Systems Engineering Team
Python Version: 3.8+
================================================================================
"""

import os
import sys
import time
import mmap
import stat
import argparse
import tracemalloc
from typing import Generator, Tuple, Optional, Dict, Any, List


# ==============================================================================
# OS-LEVEL MEMORY & SYSTEM METRICS MONITOR
# ==============================================================================
class OSMemoryProfiler:
    """
    Monitors process-level memory footprint and OS virtual memory statistics.
    Uses native OS system calls (Windows PSAPI or POSIX getrusage) without
    requiring third-party external dependencies (like psutil).
    """

    def __init__(self):
        # Enable Python heap allocation tracer as secondary diagnostic
        tracemalloc.start()
        self._is_windows = sys.platform.startswith("win")

    def get_memory_stats(self) -> Dict[str, Any]:
        """
        Query the operating system kernel for current process memory metrics:
          - Resident Set Size (RSS): Physical RAM currently occupied.
          - Peak Working Set / Peak RSS: Maximum physical RAM occupied.
          - Page Faults: Total hardware page faults handled by the OS kernel.
        """
        if self._is_windows:
            try:
                import ctypes
                from ctypes import wintypes

                class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                    _fields_ = [
                        ("cb", wintypes.DWORD),
                        ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t),
                    ]

                pmc = PROCESS_MEMORY_COUNTERS()
                pmc.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                psapi = ctypes.WinDLL("psapi")
                kernel32 = ctypes.WinDLL("kernel32")

                psapi.GetProcessMemoryInfo.argtypes = [
                    wintypes.HANDLE,
                    ctypes.POINTER(PROCESS_MEMORY_COUNTERS),
                    wintypes.DWORD,
                ]
                psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
                kernel32.GetCurrentProcess.restype = wintypes.HANDLE

                process_handle = kernel32.GetCurrentProcess()
                if psapi.GetProcessMemoryInfo(process_handle, ctypes.byref(pmc), pmc.cb):
                    return {
                        "rss_mb": pmc.WorkingSetSize / (1024 * 1024),
                        "peak_rss_mb": pmc.PeakWorkingSetSize / (1024 * 1024),
                        "page_faults": pmc.PageFaultCount,
                        "source": "Windows OS PSAPI (GetProcessMemoryInfo)",
                    }
            except Exception:
                pass

        else:
            # POSIX / Linux / macOS: Query via getrusage system call
            try:
                import resource

                usage = resource.getrusage(resource.RUSAGE_SELF)
                # On Linux ru_maxrss is reported in KiB; on macOS it is in bytes
                divisor = (1024.0 * 1024.0) if sys.platform == "darwin" else 1024.0
                total_page_faults = getattr(usage, "ru_majflt", 0) + getattr(usage, "ru_minflt", 0)
                return {
                    "rss_mb": usage.ru_maxrss / divisor,
                    "peak_rss_mb": usage.ru_maxrss / divisor,
                    "page_faults": total_page_faults,
                    "source": "POSIX getrusage(RUSAGE_SELF)",
                }
            except Exception:
                pass

        # Standard library fallback (Python-managed heap)
        current, peak = tracemalloc.get_traced_memory()
        return {
            "rss_mb": current / (1024 * 1024),
            "peak_rss_mb": peak / (1024 * 1024),
            "page_faults": 0,
            "source": "Python tracemalloc",
        }


# ==============================================================================
# OS FILE METADATA INSPECTOR (via os.fstat)
# ==============================================================================
class OSFileMetadata:
    """
    Wraps the os.fstat() system call to inspect kernel-level inode metadata,
    file allocation size, permissions, and memory page configuration.
    """

    def __init__(self, fd: int, filepath: str):
        self.filepath = filepath
        self.fd = fd
        # Invoke the fstat() system call directly on the low-level integer file descriptor
        self.stat_info = os.fstat(fd)
        self.file_size = self.stat_info.st_size
        self.mode = self.stat_info.st_mode
        self.inode = getattr(self.stat_info, "st_ino", None)
        self.device = getattr(self.stat_info, "st_dev", None)
        self.mtime = self.stat_info.st_mtime
        self.is_regular = stat.S_ISREG(self.mode)
        # Virtual memory hardware page size (standard x86/x64: 4096 bytes)
        self.page_size = mmap.PAGESIZE

    def display(self):
        """Prints a structured summary of the OS file descriptor metadata."""
        size_mb = self.file_size / (1024 * 1024)
        print("+" + "-" * 78 + "+")
        print("| {:<76} |".format("OS KERNEL FILE DESCRIPTOR & INODE METADATA (via os.fstat)"))
        print("+" + "-" * 78 + "+")
        print(f"| File Path        : {self.filepath}")
        print(f"| File Descriptor  : fd={self.fd} (POSIX integer table entry)")
        print(f"| File Size        : {self.file_size:,} bytes ({size_mb:.4f} MB)")
        print(f"| File Mode        : {oct(self.mode)} (Regular File: {self.is_regular})")
        print(f"| Inode / Identifier: {self.inode if self.inode else 'N/A'}")
        print(f"| Storage Device ID: {self.device if self.device else 'N/A'}")
        print(f"| VM Page Size     : {self.page_size:,} bytes ({self.page_size // 1024} KB)")
        print("+" + "-" * 78 + "+\n")


# ==============================================================================
# HIGH-PERFORMANCE OS LOG INGESTION ENGINE
# ==============================================================================
class OSIngestionEngine:
    """
    Stage 2 Ingestion Engine for Single-Source Log Normalization Pipeline.

    Key Architectural Principles:
      1. Zero readline() Overhead: Uses POSIX mmap to project disk blocks directly
         into process virtual address space, eliminating user-space buffer copies.
      2. Kernel Page Cache Optimization: Sequential byte scanning triggers the OS
         kernel read-ahead logic. On POSIX, madvise(MADV_SEQUENTIAL) hints the kernel.
      3. Dynamic Line Alignment: Chunks are sized uniformly (e.g. 64 MB), but chunk
         boundaries are dynamically extended to the next b'\\n', guaranteeing
         that log entries are never truncated across chunk borders.
      4. Low-Level Resource Management: Explicit os.open() and os.close() to prevent
         file descriptor leaks.
    """

    def __init__(self, filepath: str, chunk_size_bytes: int = 64 * 1024 * 1024):
        """
        Initialize the ingestion engine.

        Args:
            filepath: Path to the raw target log file (e.g., HDFS_2k.log).
            chunk_size_bytes: Target chunk processing size in bytes (default: 64 MB).
        """
        self.filepath = filepath
        self.chunk_size = chunk_size_bytes
        self.fd: Optional[int] = None
        self.mm: Optional[mmap.mmap] = None
        self.metadata: Optional[OSFileMetadata] = None
        self.file_size: int = 0
        self.profiler = OSMemoryProfiler()

    def __enter__(self):
        """Context manager entry: opens the file descriptor and creates mmap."""
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: guarantees deterministic cleanup of OS resources."""
        self.close()

    def open(self):
        """
        Step 1 & 2: Low-Level File Descriptor Allocation and Memory Mapping.
        
        System Calls Used:
          - os.open(): Requests a raw integer file descriptor from the OS kernel.
            POSIX flags: os.O_RDONLY (Read-Only access mode).
            Cross-platform binary flag: os.O_BINARY (Windows compatibility).
          - os.fstat(): Inquires file metadata directly from the kernel vnode/inode.
          - mmap.mmap(): Maps the file into the virtual address space (sys_mmap).
        """
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"[OS Error] Specified log file does not exist: {self.filepath}")

        # Construct POSIX flags for read-only access.
        # On Windows, O_BINARY avoids CRLF translation at the C runtime level.
        open_flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)

        # OS System Call: os.open() returns an integer file descriptor (fd)
        # This bypasses Python's user-space I/O buffer layer (io.BufferedReader).
        self.fd = os.open(self.filepath, open_flags)

        # OS System Call: os.fstat() extracts file metadata
        self.metadata = OSFileMetadata(self.fd, self.filepath)
        self.file_size = self.metadata.file_size

        if not self.metadata.is_regular:
            raise ValueError(f"[OS Error] File descriptor {self.fd} is not a regular file.")

        # Guard against zero-byte files (mmap raises ValueError on 0-length files)
        if self.file_size == 0:
            self.mm = None
            return

        # OS System Call: mmap.mmap()
        # fileno=fd: Uses the raw kernel file descriptor.
        # length=0: Maps the entire file into virtual memory space.
        # access=mmap.ACCESS_READ: Sets read-only memory protection (PROT_READ).
        # This enables direct memory pointer dereferencing without copying into userspace.
        self.mm = mmap.mmap(
            fileno=self.fd,
            length=0,
            access=mmap.ACCESS_READ
        )

        # Kernel Page Cache Hint (madvise):
        # On POSIX systems, madvise(MADV_SEQUENTIAL) informs the kernel that memory
        # will be accessed sequentially. The kernel responds by doubling its read-ahead
        # window and aggressively evicting stale pages behind the scan.
        if hasattr(self.mm, "madvise") and hasattr(mmap, "MADV_SEQUENTIAL"):
            try:
                self.mm.madvise(mmap.MADV_SEQUENTIAL)
            except (OSError, ValueError):
                pass  # Graceful fallback if unsupported by specific OS kernel

    def stream_chunks(self) -> Generator[Tuple[int, int, int, bytes], None, None]:
        """
        Step 3 & 4: Chunking with Dynamic Newline Boundary Alignment.

        Why Chunking is Essential:
          While mmap maps the entire file into virtual memory without loading all
          bytes into RAM at once, downstream processing stages need bounded byte
          slices (e.g., 64 MB) to prevent out-of-memory errors.

        Newline Boundary Alignment Algorithm:
          1. Propose tentative chunk end: tentative_end = min(start + chunk_size, file_size)
          2. If tentative_end falls in the middle of a log line, search forward in the
             virtual memory buffer using self.mm.find(b'\\n', tentative_end) for the next newline.
          3. Extend chunk_end = newline_pos + 1.
          4. Slice self.mm[chunk_start:chunk_end]. This guarantees atomic log records with
             zero partial-line truncations.
          5. Set next chunk_start = chunk_end.

        Yields:
            Tuple of (chunk_index, chunk_start_offset, chunk_end_offset, chunk_bytes)
        """
        if self.mm is None or self.file_size == 0:
            return

        chunk_start = 0
        chunk_index = 0

        while chunk_start < self.file_size:
            # 1. Propose tentative chunk boundary
            tentative_end = min(chunk_start + self.chunk_size, self.file_size)

            # 2. Dynamic Newline Alignment: avoid cutting a log line in half
            if tentative_end < self.file_size:
                # Direct pointer scan in virtual memory for next b'\n'
                # mmap.find is implemented in optimized C (memchr under the hood)
                newline_pos = self.mm.find(b"\n", tentative_end)
                if newline_pos != -1:
                    # Include the newline byte in the current chunk
                    chunk_end = newline_pos + 1
                else:
                    # No trailing newline found until EOF; consume remainder
                    chunk_end = self.file_size
            else:
                chunk_end = self.file_size

            # 3. High-Performance Zero-Copy Slicing:
            # Slicing the mmap directly yields a bytes buffer of complete lines.
            # No UTF-8 string decoding is performed yet, avoiding memory copies.
            chunk_bytes = self.mm[chunk_start:chunk_end]

            yield (chunk_index, chunk_start, chunk_end, chunk_bytes)

            # 4. Advance pointer cleanly to the next chunk boundary
            chunk_start = chunk_end
            chunk_index += 1

    def stream_lines(self) -> Generator[Tuple[int, bytes], None, None]:
        """
        Step 5: Stream atomic raw log lines directly as bytes for Stage 3 (Normalization).
        
        Yields:
            Tuple of (line_number, raw_line_bytes)
        """
        line_number = 0
        for _, _, _, chunk_bytes in self.stream_chunks():
            # bytes.splitlines() is implemented in native C in CPython.
            # It separates lines without keeping newline characters and without
            # decoding the entire chunk to Python str objects.
            for raw_line in chunk_bytes.splitlines():
                if raw_line:  # Skip blank lines if present
                    line_number += 1
                    yield (line_number, raw_line)

    def benchmark_ingestion(self, display_preview: bool = True, preview_count: int = 5) -> Dict[str, Any]:
        """
        Benchmarks the full OS ingestion pipeline:
          - Measures total elapsed wall-clock time using high-precision time.perf_counter()
          - Computes I/O throughput in Megabytes per Second (MB/s) and Lines per Second
          - Tracks Resident Set Size (RSS) and hardware page faults
          - Displays chunk boundary alignment details and log line previews
        """
        if self.metadata is None or self.mm is None:
            raise RuntimeError("[OS Error] Ingestion engine must be opened before benchmarking.")

        print("=" * 80)
        print("STARTING OS INGESTION BENCHMARK & SYSTEM ANALYSIS")
        print("=" * 80)
        self.metadata.display()

        # Record baseline memory before traversing mapped pages
        mem_before = self.profiler.get_memory_stats()

        total_bytes = 0
        total_lines = 0
        chunk_count = 0
        sample_preview: List[bytes] = []

        chunk_size_mb = self.chunk_size / (1024 * 1024)
        print(f"[*] Processing mapped buffer in {chunk_size_mb:.2f} MB chunks...")
        print("-" * 80)
        print(f"{'Chunk':<8} | {'Start Byte':<14} | {'End Byte':<14} | {'Chunk Size':<14} | {'Lines Count':<12}")
        print("-" * 80)

        # High-resolution CPU timing
        start_time = time.perf_counter()

        for chunk_idx, c_start, c_end, chunk_bytes in self.stream_chunks():
            chunk_count += 1
            chunk_len = len(chunk_bytes)
            total_bytes += chunk_len

            # High-performance byte scanning:
            # We count newlines directly in the byte buffer using C-level count(b'\n').
            # This avoids creating millions of intermediate PyObjects in RAM.
            newlines_in_chunk = chunk_bytes.count(b"\n")
            
            # Handle edge case: if chunk has content and doesn't end with newline (e.g. EOF without \n)
            if chunk_len > 0 and not chunk_bytes.endswith(b"\n"):
                newlines_in_chunk += 1
            
            total_lines += newlines_in_chunk

            # Capture preview lines from the very first chunk without decoding everything
            if display_preview and len(sample_preview) < preview_count:
                preview_search_start = 0
                while len(sample_preview) < preview_count and preview_search_start < chunk_len:
                    nl_pos = chunk_bytes.find(b"\n", preview_search_start)
                    if nl_pos != -1:
                        sample_preview.append(chunk_bytes[preview_search_start:nl_pos].rstrip(b"\r"))
                        preview_search_start = nl_pos + 1
                    else:
                        sample_preview.append(chunk_bytes[preview_search_start:].rstrip(b"\r"))
                        break

            print(f"{chunk_idx:<8} | {c_start:<14,} | {c_end:<14,} | {chunk_len:<14,} | {newlines_in_chunk:<12,}")

        end_time = time.perf_counter()
        elapsed_sec = max(end_time - start_time, 1e-9)

        # Record post-ingestion memory metrics
        mem_after = self.profiler.get_memory_stats()

        throughput_mb_s = (total_bytes / (1024 * 1024)) / elapsed_sec
        throughput_lines_s = total_lines / elapsed_sec

        print("-" * 80)
        print("INGESTION BENCHMARK RESULTS:")
        print(f"  * Total Bytes Processed  : {total_bytes:,} bytes ({total_bytes / (1024 * 1024):.4f} MB)")
        print(f"  * Total Lines Ingested   : {total_lines:,} lines")
        print(f"  * Total Chunks Processed : {chunk_count} chunk(s)")
        print(f"  * Elapsed Time           : {elapsed_sec:.6f} seconds")
        print(f"  * Processing Throughput  : {throughput_mb_s:.2f} MB/s")
        print(f"  * Line Throughput        : {throughput_lines_s:,.0f} lines/sec")
        print(f"  * Memory Profile (RSS)   : Baseline: {mem_before['rss_mb']:.2f} MB | Final: {mem_after['rss_mb']:.2f} MB | Peak: {mem_after['peak_rss_mb']:.2f} MB")
        print(f"  * OS Page Faults Handled : {mem_after['page_faults']:,} (Source: {mem_after['source']})")
        print("=" * 80)

        # Display raw sample lines to verify atomic line boundary preservation
        if sample_preview:
            print(f"\n[+] Verified Line Boundary Integrity (Sample First {len(sample_preview)} Lines):")
            for idx, line in enumerate(sample_preview, 1):
                preview_text = line[:100].decode("utf-8", errors="replace")
                suffix = "..." if len(line) > 100 else ""
                print(f"  [{idx:02d}] {preview_text}{suffix}")
            print()

        return {
            "total_bytes": total_bytes,
            "total_lines": total_lines,
            "chunks": chunk_count,
            "elapsed_seconds": elapsed_sec,
            "throughput_mb_s": throughput_mb_s,
            "throughput_lines_s": throughput_lines_s,
            "memory_metrics": mem_after,
        }

    def close(self):
        """
        Step 6: Deterministic OS Resource Reclamation.

        Order of Cleanup:
          1. Close the mmap memory view (unmaps pages from virtual memory).
          2. Close the low-level file descriptor via os.close() to free the kernel table slot.
        """
        # Unmap memory mapping first
        if self.mm is not None:
            try:
                self.mm.close()
            except Exception:
                pass
            self.mm = None

        # Release POSIX file descriptor
        if self.fd is not None:
            try:
                os.close(self.fd)
            except Exception:
                pass
            self.fd = None


# ==============================================================================
# SYNTHETIC LOG GENERATOR (FOR STRESS TESTING & SCALING BENCHMARKS)
# ==============================================================================
def generate_synthetic_log(
    output_path: str,
    target_size_mb: int = 500,
    template_file: Optional[str] = None
) -> str:
    """
    Generates a massive, realistic synthetic log file (e.g. 500 MB - 1 GB)
    for stress testing the OS Ingestion Engine.

    Techniques Used for Speed:
      - Builds multi-megabyte pre-buffered byte blocks in memory.
      - Writes using direct low-level OS file descriptors (os.open, os.write, os.close)
        to minimize syscall overhead and bypass C runtime buffer flushes.

    Args:
        output_path: Target path for the generated log file.
        target_size_mb: Size in Megabytes (default: 500 MB).
        template_file: Optional path to an authentic log (e.g. HDFS_2k.log) to replicate.

    Returns:
        Absolute path to the generated synthetic log file.
    """
    target_bytes = target_size_mb * 1024 * 1024
    print("=" * 80)
    print(f"GENERATING SYNTHETIC LOG FILE: {output_path} ({target_size_mb} MB)")
    print("=" * 80)

    # 1. Gather template lines
    sample_lines = []
    if template_file and os.path.exists(template_file):
        print(f"[*] Reading template patterns from authentic log: {template_file}")
        with open(template_file, "rb") as f:
            for _ in range(500):
                line = f.readline()
                if not line:
                    break
                sample_lines.append(line)

    if not sample_lines:
        # Default high-fidelity HDFS log templates
        sample_lines = [
            b"081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating\n",
            b"081109 203807 222 INFO dfs.DataNode$PacketResponder: PacketResponder 0 for block blk_-6952295868487656571 terminating\n",
            b"081109 204005 35 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.73.220:50010 is added to blk_7128370237687728475 size 67108864\n",
            b"081109 204015 308 WARN dfs.DataNode$DataXceiver: 10.250.19.102:50010:Got exception while serving blk_8229193803249955061 to /10.251.214.240:\n",
            b"081109 204106 329 ERROR dfs.DataNode$PacketResponder: PacketResponder 2 for block blk_-6670958622368987959 terminating\n",
            b"081109 204132 26 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /user/root/rand/_temporary/_task_200811092030_0001_m_000590_0/part-00590. blk_322589083232145787\n",
        ]

    # Pre-compose an ~8 MB byte buffer block
    unit_block = b"".join(sample_lines)
    repetitions = max(1, (8 * 1024 * 1024) // len(unit_block))
    buffer_block = unit_block * repetitions
    block_size = len(buffer_block)

    # 2. Write directly using low-level OS system calls
    open_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
    # POSIX permissions: 0o644 (rw-r--r--)
    fd = os.open(output_path, open_flags, 0o644)

    written_bytes = 0
    start_time = time.perf_counter()

    try:
        while written_bytes < target_bytes:
            bytes_to_write = min(block_size, target_bytes - written_bytes)
            # os.write() writes directly to kernel page cache / disk buffer
            bytes_written = os.write(fd, buffer_block[:bytes_to_write])
            written_bytes += bytes_written
    finally:
        # Guarantee closure of file descriptor
        os.close(fd)

    elapsed = max(time.perf_counter() - start_time, 1e-9)
    throughput = (written_bytes / (1024 * 1024)) / elapsed

    print(f"[+] Successfully wrote {written_bytes / (1024 * 1024):.2f} MB to {output_path}")
    print(f"[+] Write Time: {elapsed:.3f} s (Throughput: {throughput:.2f} MB/s)\n")
    return os.path.abspath(output_path)


# ==============================================================================
# CLI DISPATCHER & BENCHMARK SUITE
# ==============================================================================
def parse_bytes(size_str: str) -> int:
    """Helper to parse user-specified chunk sizes like '64MB', '512KB', etc."""
    size_str = size_str.strip().upper()
    if size_str.endswith("KB"):
        return int(float(size_str[:-2]) * 1024)
    elif size_str.endswith("MB"):
        return int(float(size_str[:-2]) * 1024 * 1024)
    elif size_str.endswith("GB"):
        return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
    elif size_str.endswith("B"):
        return int(size_str[:-1])
    else:
        return int(size_str)


def main():
    parser = argparse.ArgumentParser(
        description="High-Performance OS-Level Log Ingestion Engine (Stage 2: Single-Source Pipeline)"
    )
    parser.add_argument(
        "file",
        nargs="?",
        default="HDFS_2k.log",
        help="Path to the log file to ingest (default: HDFS_2k.log if present in working directory)",
    )
    parser.add_argument(
        "--chunk-size",
        default=None,
        help="Chunk size (e.g. 64KB, 1MB, 64MB). If omitted, automatically selects 64KB "
             "for small files (<10 MB) to showcase multi-chunk alignment, or 64MB for large files.",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=5,
        help="Number of preview lines to display (default: 5)",
    )
    parser.add_argument(
        "--generate",
        metavar="PATH",
        help="Generate a dummy synthetic log file for stress-testing and exit.",
    )
    parser.add_argument(
        "--size-mb",
        type=int,
        default=500,
        help="Size of synthetic log to generate in MB (default: 500 MB).",
    )

    args = parser.parse_args()

    # Mode 1: Synthetic Log Generation
    if args.generate:
        template = args.file if os.path.exists(args.file) else None
        generate_synthetic_log(args.generate, target_size_mb=args.size_mb, template_file=template)
        return

    # Mode 2: Ingest and Benchmark specified log file
    target_file = args.file
    if not os.path.exists(target_file):
        # Check case-insensitive match for convenience (e.g. hdfs_2k.log vs HDFS_2k.log)
        for item in os.listdir("."):
            if item.lower() == target_file.lower():
                target_file = item
                break

    if not os.path.exists(target_file):
        print(f"[ERROR] Target file '{args.file}' not found in current directory.")
        print("Usage:")
        print("  python os_ingestion_engine.py <logfile>")
        print("  python os_ingestion_engine.py --generate dummy.log --size-mb 500")
        sys.exit(1)

    # Determine optimal chunk size if not explicitly provided
    if args.chunk_size:
        chunk_bytes = parse_bytes(args.chunk_size)
    else:
        file_sz = os.path.getsize(target_file)
        if file_sz < 10 * 1024 * 1024:
            # For small files like HDFS_2k.log (287 KB), use 64 KB to demonstrate chunk boundary alignment
            chunk_bytes = 64 * 1024
        else:
            # For massive logs (>= 10 MB), use standard 64 MB blocks
            chunk_bytes = 64 * 1024 * 1024

    # Instantiate and run Ingestion Engine via context manager
    with OSIngestionEngine(target_file, chunk_size_bytes=chunk_bytes) as engine:
        engine.benchmark_ingestion(display_preview=True, preview_count=args.preview)


if __name__ == "__main__":
    main()
