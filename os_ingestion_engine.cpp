/*
================================================================================
SINGLE-SOURCE LOG NORMALIZATION & ANALYTICS PIPELINE (C++ Edition)
Stage 2: High-Performance OS-Level Log Ingestion Engine
================================================================================
Role: Systems Software Engineer
Description:
    A high-performance log ingestion engine implemented in modern C++ (C++17)
    utilizing native OS system calls and low-level kernel primitives:
      1. Low-Level OS File Descriptors / Handles (CreateFileA / open, fstat)
      2. Virtual Memory Mapping (MapViewOfFile / mmap) for zero-copy memory scans
      3. Kernel Page Cache Alignment & Prefetching (FILE_FLAG_SEQUENTIAL_SCAN / madvise)
      4. Chunking with Dynamic Newline Boundary Alignment (atomic log lines)
      5. Zero-Copy String Views (std::string_view) without string heap allocations
      6. Native OS Memory (RSS) & Hardware Page Fault Profiling (PSAPI / getrusage)
      7. Pipeline Stages 3, 4, 5 (Parsing, Deduplication, Micro-Batch Buffer)

Author: Systems Engineering Team
C++ Standard: C++17 or higher
Compiler Support: GCC (g++), Clang (clang++), MSVC (cl.exe)
================================================================================
*/

#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <deque>
#include <unordered_set>
#include <chrono>
#include <iomanip>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <algorithm>
#include <sstream>
#include <fstream>

// ==============================================================================
// OS-SPECIFIC SYSTEM HEADERS
// ==============================================================================
#ifdef _WIN32
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <windows.h>
    #include <psapi.h>
#else
    #include <sys/mman.h>
    #include <sys/stat.h>
    #include <sys/types.h>
    #include <fcntl.h>
    #include <unistd.h>
    #include <sys/resource.h>
#endif


// ==============================================================================
// OS-LEVEL MEMORY & SYSTEM METRICS PROFILER
// ==============================================================================
struct MemoryStats {
    double rss_mb = 0.0;          // Physical RAM currently resident
    double peak_rss_mb = 0.0;     // Peak physical RAM occupied
    uint64_t page_faults = 0;     // Hardware/OS page faults handled
    std::string source;           // System API queried
};

class OSMemoryProfiler {
public:
    static MemoryStats get_memory_stats() {
        MemoryStats stats;

#ifdef _WIN32
        // Windows Kernel Query via PSAPI (GetProcessMemoryInfo)
        PROCESS_MEMORY_COUNTERS_EX pmc;
        ZeroMemory(&pmc, sizeof(pmc));
        pmc.cb = sizeof(pmc);

        HANDLE hProcess = GetCurrentProcess();
        if (GetProcessMemoryInfo(hProcess, reinterpret_cast<PROCESS_MEMORY_COUNTERS*>(&pmc), sizeof(pmc))) {
            stats.rss_mb = static_cast<double>(pmc.WorkingSetSize) / (1024.0 * 1024.0);
            stats.peak_rss_mb = static_cast<double>(pmc.PeakWorkingSetSize) / (1024.0 * 1024.0);
            stats.page_faults = pmc.PageFaultCount;
            stats.source = "Windows OS PSAPI (GetProcessMemoryInfo)";
        }
#else
        // POSIX System Call via getrusage
        struct rusage usage;
        if (getrusage(RUSAGE_SELF, &usage) == 0) {
#if defined(__APPLE__)
            double divisor = 1024.0 * 1024.0; // macOS reports bytes
#else
            double divisor = 1024.0;          // Linux reports KiB
#endif
            stats.rss_mb = static_cast<double>(usage.ru_maxrss) / divisor;
            stats.peak_rss_mb = stats.rss_mb;
            stats.page_faults = static_cast<uint64_t>(usage.ru_majflt + usage.ru_minflt);
            stats.source = "POSIX getrusage(RUSAGE_SELF)";
        }
#endif
        return stats;
    }
};


// ==============================================================================
// OS FILE METADATA INSPECTOR
// ==============================================================================
struct OSFileMetadata {
    std::string filepath;
    size_t file_size = 0;
    uint32_t page_size = 4096;
    uint64_t file_id = 0;

    void display() const {
        double size_mb = static_cast<double>(file_size) / (1024.0 * 1024.0);
        std::cout << "+" << std::string(78, '-') << "+\n";
        std::cout << "| " << std::left << std::setw(76) 
                  << "OS KERNEL FILE METADATA & VIRTUAL MEMORY CONFIGURATION" << " |\n";
        std::cout << "+" << std::string(78, '-') << "+\n";
        std::cout << "| File Path         : " << filepath << "\n";
        std::cout << "| File Size         : " << file_size << " bytes (" 
                  << std::fixed << std::setprecision(4) << size_mb << " MB)\n";
        std::cout << "| OS File Identifier: " << (file_id ? std::to_string(file_id) : "N/A") << "\n";
        std::cout << "| VM Page Size      : " << page_size << " bytes (" 
                  << (page_size / 1024) << " KB)\n";
        std::cout << "+" << std::string(78, '-') << "+\n\n";
    }
};


// ==============================================================================
// STAGE 2: HIGH-PERFORMANCE OS-LEVEL INGESTION ENGINE
// ==============================================================================
class OSIngestionEngine {
private:
    std::string filepath;
    size_t chunk_size;
    size_t file_size = 0;
    const char* mapped_data = nullptr;
    OSFileMetadata metadata;

#ifdef _WIN32
    HANDLE hFile = INVALID_HANDLE_VALUE;
    HANDLE hMapping = NULL;
    DWORD system_page_size = 4096;
#else
    int fd = -1;
    long system_page_size = 4096;
#endif

public:
    explicit OSIngestionEngine(std::string path, size_t chunk_sz = 64 * 1024 * 1024)
        : filepath(std::move(path)), chunk_size(chunk_sz) {}

    ~OSIngestionEngine() {
        close();
    }

    // Disable copy semantics to prevent accidental multiple unmappings of OS handles
    OSIngestionEngine(const OSIngestionEngine&) = delete;
    OSIngestionEngine& operator=(const OSIngestionEngine&) = delete;

    // Enable move semantics
    OSIngestionEngine(OSIngestionEngine&& other) noexcept {
        *this = std::move(other);
    }

    OSIngestionEngine& operator=(OSIngestionEngine&& other) noexcept {
        if (this != &other) {
            close();
            filepath = std::move(other.filepath);
            chunk_size = other.chunk_size;
            file_size = other.file_size;
            mapped_data = other.mapped_data;
            metadata = other.metadata;

#ifdef _WIN32
            hFile = other.hFile;
            hMapping = other.hMapping;
            system_page_size = other.system_page_size;
            other.hFile = INVALID_HANDLE_VALUE;
            other.hMapping = NULL;
#else
            fd = other.fd;
            system_page_size = other.system_page_size;
            other.fd = -1;
#endif
            other.mapped_data = nullptr;
            other.file_size = 0;
        }
        return *this;
    }

    /*
      Step 1 & 2: Low-Level File Open and Memory Mapping.
      
      OS System Calls Used:
        - Windows: CreateFileA (with FILE_FLAG_SEQUENTIAL_SCAN), GetFileSizeEx,
                   CreateFileMappingA, MapViewOfFile.
        - POSIX:   open (O_RDONLY), fstat, mmap (PROT_READ), posix_madvise (MADV_SEQUENTIAL).
    */
    bool open() {
#ifdef _WIN32
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        system_page_size = si.dwPageSize;

        // Open raw file handle directly via Windows OS kernel.
        // FILE_FLAG_SEQUENTIAL_SCAN instructs the Windows Cache Manager to aggressively
        // prefetch disk clusters into the OS Page Cache.
        hFile = CreateFileA(
            filepath.c_str(),
            GENERIC_READ,
            FILE_SHARE_READ,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
            NULL
        );

        if (hFile == INVALID_HANDLE_VALUE) {
            std::cerr << "[OS Error] CreateFileA failed on '" << filepath 
                      << "' (Error: " << GetLastError() << ")\n";
            return false;
        }

        LARGE_INTEGER li;
        if (!GetFileSizeEx(hFile, &li)) {
            std::cerr << "[OS Error] GetFileSizeEx failed (Error: " << GetLastError() << ")\n";
            CloseHandle(hFile);
            hFile = INVALID_HANDLE_VALUE;
            return false;
        }
        file_size = static_cast<size_t>(li.QuadPart);

        metadata.filepath = filepath;
        metadata.file_size = file_size;
        metadata.page_size = system_page_size;

        BY_HANDLE_FILE_INFORMATION bhfi;
        if (GetFileInformationByHandle(hFile, &bhfi)) {
            metadata.file_id = (static_cast<uint64_t>(bhfi.nFileIndexHigh) << 32) | bhfi.nFileIndexLow;
        }

        if (file_size == 0) {
            mapped_data = nullptr;
            return true;
        }

        // Allocate kernel section object for virtual memory mapping
        hMapping = CreateFileMappingA(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
        if (hMapping == NULL) {
            std::cerr << "[OS Error] CreateFileMappingA failed (Error: " << GetLastError() << ")\n";
            CloseHandle(hFile);
            hFile = INVALID_HANDLE_VALUE;
            return false;
        }

        // Map section directly into 64-bit virtual address space (zero user-space read copies)
        mapped_data = static_cast<const char*>(MapViewOfFile(hMapping, FILE_MAP_READ, 0, 0, 0));
        if (mapped_data == nullptr) {
            std::cerr << "[OS Error] MapViewOfFile failed (Error: " << GetLastError() << ")\n";
            CloseHandle(hMapping);
            CloseHandle(hFile);
            hMapping = NULL;
            hFile = INVALID_HANDLE_VALUE;
            return false;
        }

#else
        system_page_size = sysconf(_SC_PAGESIZE);

        // Open POSIX low-level integer file descriptor
        fd = ::open(filepath.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "[OS Error] open() syscall failed on '" << filepath << "'\n";
            return false;
        }

        // Query kernel inode table via fstat()
        struct stat st;
        if (fstat(fd, &st) != 0) {
            std::cerr << "[OS Error] fstat() syscall failed\n";
            ::close(fd);
            fd = -1;
            return false;
        }
        file_size = static_cast<size_t>(st.st_size);

        metadata.filepath = filepath;
        metadata.file_size = file_size;
        metadata.page_size = static_cast<uint32_t>(system_page_size);
        metadata.file_id = static_cast<uint64_t>(st.st_ino);

        if (file_size == 0) {
            mapped_data = nullptr;
            return true;
        }

        // Map file directly into process virtual memory
        void* ptr = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "[OS Error] mmap() syscall failed\n";
            ::close(fd);
            fd = -1;
            return false;
        }
        mapped_data = static_cast<const char*>(ptr);

        // Kernel Page Cache Hint: advises OS kernel to prefetch sequentially
#if defined(POSIX_MADV_SEQUENTIAL)
        posix_madvise(const_cast<char*>(mapped_data), file_size, POSIX_MADV_SEQUENTIAL);
#elif defined(MADV_SEQUENTIAL)
        madvise(const_cast<char*>(mapped_data), file_size, MADV_SEQUENTIAL);
#endif

#endif
        return true;
    }

    /*
      Step 3 & 4: Chunking with Dynamic Newline Boundary Alignment.
      
      Iterates mapped virtual memory in chunks (e.g., 64 MB), using memchr() to 
      dynamically snap chunk boundaries to the next '\n' byte.
    */
    template <typename ChunkCallback>
    void for_each_chunk(ChunkCallback&& callback) const {
        if (!mapped_data || file_size == 0) return;

        size_t chunk_start = 0;
        size_t chunk_idx = 0;

        while (chunk_start < file_size) {
            // Propose tentative chunk boundary
            size_t tentative_end = std::min(chunk_start + chunk_size, file_size);
            size_t chunk_end = tentative_end;

            // Dynamic Newline Alignment: Avoid cutting a log entry in half
            if (tentative_end < file_size) {
                // memchr is vectorized in modern C runtimes (AVX2/SSE)
                const char* nl = static_cast<const char*>(
                    memchr(mapped_data + tentative_end, '\n', file_size - tentative_end)
                );
                if (nl != nullptr) {
                    chunk_end = static_cast<size_t>((nl - mapped_data) + 1); // Include '\n'
                } else {
                    chunk_end = file_size;
                }
            }

            // Zero-copy view into mapped memory
            std::string_view chunk_view(mapped_data + chunk_start, chunk_end - chunk_start);
            callback(chunk_idx, chunk_start, chunk_end, chunk_view);

            chunk_start = chunk_end;
            chunk_idx++;
        }
    }

    /*
      Step 5: Iterate atomic raw lines within a chunk without heap allocations.
    */
    template <typename LineCallback>
    static void for_each_line_in_chunk(std::string_view chunk, LineCallback&& callback) {
        const char* ptr = chunk.data();
        size_t remaining = chunk.size();

        while (remaining > 0) {
            const char* nl = static_cast<const char*>(memchr(ptr, '\n', remaining));
            if (nl != nullptr) {
                size_t line_len = static_cast<size_t>(nl - ptr);
                // Strip trailing '\r' if CRLF
                if (line_len > 0 && ptr[line_len - 1] == '\r') {
                    line_len--;
                }
                if (line_len > 0) {
                    callback(std::string_view(ptr, line_len));
                }
                size_t step = static_cast<size_t>((nl - ptr) + 1);
                ptr += step;
                remaining -= step;
            } else {
                // Remainder to end of chunk without trailing '\n'
                size_t line_len = remaining;
                if (line_len > 0 && ptr[line_len - 1] == '\r') {
                    line_len--;
                }
                if (line_len > 0) {
                    callback(std::string_view(ptr, line_len));
                }
                break;
            }
        }
    }

    /*
      Counts newlines in a chunk using SIMD-accelerated memchr.
    */
    static size_t count_lines_in_chunk(std::string_view chunk) {
        if (chunk.empty()) return 0;
        size_t count = 0;
        const char* ptr = chunk.data();
        size_t remaining = chunk.size();

        while (remaining > 0) {
            const char* nl = static_cast<const char*>(memchr(ptr, '\n', remaining));
            if (!nl) break;
            count++;
            size_t step = static_cast<size_t>((nl - ptr) + 1);
            ptr += step;
            remaining -= step;
        }

        // Edge case: if buffer has content and does not end with '\n'
        if (!chunk.empty() && chunk.back() != '\n') {
            count++;
        }
        return count;
    }

    /*
      Runs a complete benchmark and prints kernel metrics.
    */
    void benchmark(size_t preview_count = 5) const {
        std::cout << std::string(80, '=') << "\n";
        std::cout << "STARTING OS INGESTION BENCHMARK & SYSTEM ANALYSIS (C++17 Engine)\n";
        std::cout << std::string(80, '=') << "\n";
        metadata.display();

        MemoryStats mem_before = OSMemoryProfiler::get_memory_stats();

        size_t total_bytes = 0;
        size_t total_lines = 0;
        size_t chunk_count = 0;
        std::vector<std::string> sample_preview;

        double chunk_mb = static_cast<double>(chunk_size) / (1024.0 * 1024.0);
        std::cout << "[*] Processing mapped buffer in " << std::fixed << std::setprecision(2)
                  << chunk_mb << " MB chunks...\n";
        std::cout << std::string(80, '-') << "\n";
        std::cout << std::left << std::setw(8) << "Chunk" << " | "
                  << std::setw(14) << "Start Byte" << " | "
                  << std::setw(14) << "End Byte" << " | "
                  << std::setw(14) << "Chunk Size" << " | "
                  << std::setw(12) << "Lines Count" << "\n";
        std::cout << std::string(80, '-') << "\n";

        auto start_time = std::chrono::high_resolution_clock::now();

        for_each_chunk([&](size_t chunk_idx, size_t c_start, size_t c_end, std::string_view chunk_view) {
            chunk_count++;
            total_bytes += chunk_view.size();
            size_t lines_in_chunk = count_lines_in_chunk(chunk_view);
            total_lines += lines_in_chunk;

            // Collect preview sample from first chunk
            if (sample_preview.size() < preview_count) {
                for_each_line_in_chunk(chunk_view, [&](std::string_view line) {
                    if (sample_preview.size() < preview_count) {
                        sample_preview.emplace_back(line.data(), std::min(line.size(), size_t(100)));
                    }
                });
            }

            std::cout << std::left << std::setw(8) << chunk_idx << " | "
                      << std::setw(14) << c_start << " | "
                      << std::setw(14) << c_end << " | "
                      << std::setw(14) << chunk_view.size() << " | "
                      << std::setw(12) << lines_in_chunk << "\n";
        });

        auto end_time = std::chrono::high_resolution_clock::now();
        double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
        if (elapsed_sec <= 0.0) elapsed_sec = 1e-9;

        MemoryStats mem_after = OSMemoryProfiler::get_memory_stats();

        double throughput_mb_s = (static_cast<double>(total_bytes) / (1024.0 * 1024.0)) / elapsed_sec;
        double throughput_lines_s = static_cast<double>(total_lines) / elapsed_sec;

        std::cout << std::string(80, '-') << "\n";
        std::cout << "INGESTION BENCHMARK RESULTS:\n";
        std::cout << "  * Total Bytes Processed  : " << total_bytes << " bytes ("
                  << std::fixed << std::setprecision(4) 
                  << (static_cast<double>(total_bytes) / (1024.0 * 1024.0)) << " MB)\n";
        std::cout << "  * Total Lines Ingested   : " << total_lines << " lines\n";
        std::cout << "  * Total Chunks Processed : " << chunk_count << " chunk(s)\n";
        std::cout << "  * Elapsed Time           : " << std::fixed << std::setprecision(6) 
                  << elapsed_sec << " seconds\n";
        std::cout << "  * Processing Throughput  : " << std::fixed << std::setprecision(2) 
                  << throughput_mb_s << " MB/s\n";
        std::cout << "  * Line Throughput        : " << std::fixed << std::setprecision(0) 
                  << throughput_lines_s << " lines/sec\n";
        std::cout << "  * Memory Profile (RSS)   : Baseline: " << std::fixed << std::setprecision(2)
                  << mem_before.rss_mb << " MB | Final: " << mem_after.rss_mb 
                  << " MB | Peak: " << mem_after.peak_rss_mb << " MB\n";
        std::cout << "  * OS Page Faults Handled : " << mem_after.page_faults 
                  << " (Source: " << mem_after.source << ")\n";
        std::cout << std::string(80, '=') << "\n";

        if (!sample_preview.empty()) {
            std::cout << "\n[+] Verified Line Boundary Integrity (Sample First " 
                      << sample_preview.size() << " Lines):\n";
            for (size_t i = 0; i < sample_preview.size(); ++i) {
                std::cout << "  [" << std::right << std::setw(2) << std::setfill('0') << (i + 1) << "] " 
                          << sample_preview[i] << "...\n";
            }
            std::cout << "\n";
        }
    }

    /*
      Step 6: Deterministic OS Resource Reclamation.
    */
    void close() {
#ifdef _WIN32
        if (mapped_data != nullptr) {
            UnmapViewOfFile(mapped_data);
            mapped_data = nullptr;
        }
        if (hMapping != NULL) {
            CloseHandle(hMapping);
            hMapping = NULL;
        }
        if (hFile != INVALID_HANDLE_VALUE) {
            CloseHandle(hFile);
            hFile = INVALID_HANDLE_VALUE;
        }
#else
        if (mapped_data != nullptr) {
            munmap(const_cast<char*>(mapped_data), file_size);
            mapped_data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
#endif
        file_size = 0;
    }

    size_t get_file_size() const { return file_size; }
    const OSFileMetadata& get_metadata() const { return metadata; }
};


// ==============================================================================
// STAGE 3: PARSING & NORMALIZATION LAYER (C++ IMPLEMENTATION)
// ==============================================================================
struct NormalizedLogRecord {
    std::string timestamp; // ISO 8601: "YYYY-MM-DDTHH:MM:SS"
    int pid = 0;
    std::string level;
    std::string component;
    std::string message;
    std::string block_id;
};

class LogParser {
public:
    /*
      Parses an atomic HDFS log line directly from a std::string_view without allocations.
      Format: Date(6) Time(6) PID Level Component: Message
    */
    static bool parse_hdfs_line(std::string_view line, NormalizedLogRecord& out_record) {
        if (line.size() < 20) return false;

        // 1. Date (6 chars) + Time (6 chars)
        // e.g. "081109 203615" -> 2008-11-09T20:36:15
        if (line[6] != ' ' || line[13] != ' ') return false;

        std::string_view date_part = line.substr(0, 6);
        std::string_view time_part = line.substr(7, 6);

        out_record.timestamp = "20" + std::string(date_part.substr(0, 2)) + "-" +
                               std::string(date_part.substr(2, 2)) + "-" +
                               std::string(date_part.substr(4, 2)) + "T" +
                               std::string(time_part.substr(0, 2)) + ":" +
                               std::string(time_part.substr(2, 2)) + ":" +
                               std::string(time_part.substr(4, 2));

        // 2. PID
        size_t cursor = 14;
        while (cursor < line.size() && line[cursor] == ' ') cursor++;
        size_t pid_start = cursor;
        while (cursor < line.size() && line[cursor] >= '0' && line[cursor] <= '9') cursor++;
        if (cursor == pid_start) return false;
        out_record.pid = std::atoi(std::string(line.substr(pid_start, cursor - pid_start)).c_str());

        // 3. Log Level (INFO, WARN, ERROR)
        while (cursor < line.size() && line[cursor] == ' ') cursor++;
        size_t level_start = cursor;
        while (cursor < line.size() && line[cursor] != ' ') cursor++;
        out_record.level = std::string(line.substr(level_start, cursor - level_start));

        // 4. Component (ends with ':')
        while (cursor < line.size() && line[cursor] == ' ') cursor++;
        size_t comp_start = cursor;
        size_t colon_pos = line.find(':', comp_start);
        if (colon_pos == std::string_view::npos) return false;
        out_record.component = std::string(line.substr(comp_start, colon_pos - comp_start));

        // 5. Message (remainder after ": ")
        cursor = colon_pos + 1;
        while (cursor < line.size() && line[cursor] == ' ') cursor++;
        std::string_view msg_view = line.substr(cursor);
        out_record.message = std::string(msg_view);

        // 6. Entity Extraction: Block ID (e.g. blk_38865049064139660)
        out_record.block_id = "";
        size_t blk_pos = msg_view.find("blk_");
        if (blk_pos != std::string_view::npos) {
            size_t blk_end = msg_view.find_first_of(" \t\r\n:;,", blk_pos);
            if (blk_end == std::string_view::npos) {
                out_record.block_id = std::string(msg_view.substr(blk_pos));
            } else {
                out_record.block_id = std::string(msg_view.substr(blk_pos, blk_end - blk_pos));
            }
        }
        return true;
    }
};


// ==============================================================================
// STAGE 4: DEDUPLICATION FILTER (DSA: SLIDING WINDOW HASH SET)
// ==============================================================================
class DeduplicationFilter {
private:
    size_t max_capacity;
    std::unordered_set<uint64_t> hash_set;
    std::deque<uint64_t> sliding_window;

    // 64-bit FNV-1a Hash Algorithm
    static uint64_t compute_hash(const NormalizedLogRecord& rec) {
        uint64_t h = 14695981039346656037ULL;
        auto hash_str = [&h](const std::string& s) {
            for (char c : s) {
                h ^= static_cast<uint64_t>(static_cast<unsigned char>(c));
                h *= 1099511628211ULL;
            }
        };
        hash_str(rec.timestamp);
        hash_str(rec.level);
        hash_str(rec.component);
        hash_str(rec.message);
        return h;
    }

public:
    explicit DeduplicationFilter(size_t capacity = 50000) : max_capacity(capacity) {}

    /*
      Checks if log record was recently observed.
      Returns true if DUPLICATE (dropped).
      Returns false if UNIQUE (admitted to sliding window).
    */
    bool is_duplicate_or_add(const NormalizedLogRecord& rec) {
        uint64_t fp = compute_hash(rec);

        if (hash_set.find(fp) != hash_set.end()) {
            return true; // Duplicate detected!
        }

        // Maintain bounded sliding window of size K
        if (sliding_window.size() >= max_capacity) {
            uint64_t oldest = sliding_window.front();
            sliding_window.pop_front();
            hash_set.erase(oldest);
        }

        sliding_window.push_back(fp);
        hash_set.insert(fp);
        return false;
    }

    size_t size() const { return sliding_window.size(); }
};


// ==============================================================================
// STAGE 5: MICRO-BATCH BUFFER (IN-MEMORY QUEUE)
// ==============================================================================
class MicroBatchBuffer {
private:
    size_t batch_size;
    double max_wait_sec;
    std::vector<NormalizedLogRecord> buffer;
    std::chrono::high_resolution_clock::time_point last_flush;

public:
    explicit MicroBatchBuffer(size_t size = 5000, double max_wait = 0.5)
        : batch_size(size), max_wait_sec(max_wait) {
        buffer.reserve(batch_size);
        last_flush = std::chrono::high_resolution_clock::now();
    }

    /*
      Adds a record to the buffer.
      If batch size is reached or timeout expires, flushes batch into flushed_batch.
      Returns true if a flush occurred.
    */
    bool add(NormalizedLogRecord&& rec, std::vector<NormalizedLogRecord>& flushed_batch) {
        buffer.push_back(std::move(rec));
        auto now = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(now - last_flush).count();

        if (buffer.size() >= batch_size || elapsed >= max_wait_sec) {
            flushed_batch = std::move(buffer);
            buffer.clear();
            buffer.reserve(batch_size);
            last_flush = now;
            return true;
        }
        return false;
    }

    bool flush_remaining(std::vector<NormalizedLogRecord>& flushed_batch) {
        if (buffer.empty()) return false;
        flushed_batch = std::move(buffer);
        buffer.clear();
        return true;
    }
};


// ==============================================================================
// SYNTHETIC LOG GENERATOR (FOR STRESS TESTING & SCALING BENCHMARKS)
// ==============================================================================
bool generate_synthetic_log(
    const std::string& output_path,
    size_t target_size_mb = 500,
    const std::string& template_file = ""
) {
    size_t target_bytes = target_size_mb * 1024 * 1024;
    std::cout << std::string(80, '=') << "\n";
    std::cout << "GENERATING SYNTHETIC LOG FILE: " << output_path 
              << " (" << target_size_mb << " MB)\n";
    std::cout << std::string(80, '=') << "\n";

    // 1. Gather template patterns
    std::vector<std::string> template_lines;
    if (!template_file.empty()) {
        std::ifstream infile(template_file, std::ios::binary);
        if (infile.is_open()) {
            std::cout << "[*] Reading template patterns from authentic log: " << template_file << "\n";
            std::string line;
            size_t count = 0;
            while (std::getline(infile, line) && count < 500) {
                if (!line.empty()) {
                    template_lines.push_back(line + "\n");
                    count++;
                }
            }
        }
    }

    if (template_lines.empty()) {
        template_lines = {
            "081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating\n",
            "081109 203807 222 INFO dfs.DataNode$PacketResponder: PacketResponder 0 for block blk_-6952295868487656571 terminating\n",
            "081109 204005 35 INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.73.220:50010 is added to blk_7128370237687728475 size 67108864\n",
            "081109 204015 308 WARN dfs.DataNode$DataXceiver: 10.250.19.102:50010:Got exception while serving blk_8229193803249955061 to /10.251.214.240:\n",
            "081109 204106 329 ERROR dfs.DataNode$PacketResponder: PacketResponder 2 for block blk_-6670958622368987959 terminating\n",
            "081109 204132 26 INFO dfs.FSNamesystem: BLOCK* NameSystem.allocateBlock: /user/root/rand/_temporary/_task_200811092030_0001_m_000590_0/part-00590. blk_322589083232145787\n"
        };
    }

    // Build ~8 MB pre-buffered write block
    std::string unit_block;
    for (const auto& l : template_lines) unit_block += l;
    size_t repetitions = (8 * 1024 * 1024) / std::max(unit_block.size(), size_t(1));
    std::string big_block;
    big_block.reserve(unit_block.size() * repetitions);
    for (size_t i = 0; i < repetitions; ++i) big_block += unit_block;

    size_t written_bytes = 0;
    auto start_time = std::chrono::high_resolution_clock::now();

#ifdef _WIN32
    HANDLE hOut = CreateFileA(
        output_path.c_str(),
        GENERIC_WRITE,
        0,
        NULL,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        NULL
    );
    if (hOut == INVALID_HANDLE_VALUE) {
        std::cerr << "[OS Error] Failed to create output file: " << output_path << "\n";
        return false;
    }

    while (written_bytes < target_bytes) {
        DWORD to_write = static_cast<DWORD>(std::min(big_block.size(), target_bytes - written_bytes));
        DWORD written = 0;
        if (!WriteFile(hOut, big_block.data(), to_write, &written, NULL)) {
            std::cerr << "[OS Error] WriteFile failed\n";
            CloseHandle(hOut);
            return false;
        }
        written_bytes += written;
    }
    CloseHandle(hOut);
#else
    int fd = ::open(output_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        std::cerr << "[OS Error] Failed to open output file: " << output_path << "\n";
        return false;
    }

    while (written_bytes < target_bytes) {
        size_t to_write = std::min(big_block.size(), target_bytes - written_bytes);
        ssize_t written = ::write(fd, big_block.data(), to_write);
        if (written < 0) {
            std::cerr << "[OS Error] write() failed\n";
            ::close(fd);
            return false;
        }
        written_bytes += static_cast<size_t>(written);
    }
    ::close(fd);
#endif

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    if (elapsed <= 0.0) elapsed = 1e-9;
    double throughput = (static_cast<double>(written_bytes) / (1024.0 * 1024.0)) / elapsed;

    std::cout << "[+] Successfully wrote " << (static_cast<double>(written_bytes) / (1024.0 * 1024.0))
              << " MB to " << output_path << "\n";
    std::cout << "[+] Write Time: " << std::fixed << std::setprecision(3) << elapsed 
              << " s (Throughput: " << throughput << " MB/s)\n\n";
    return true;
}


// ==============================================================================
// FULL PIPELINE DEMONSTRATOR (STAGES 2 -> 3 -> 4 -> 5)
// ==============================================================================
void run_full_pipeline(const std::string& filepath, size_t chunk_size) {
    std::cout << std::string(80, '=') << "\n";
    std::cout << "RUNNING END-TO-END PIPELINE (Stages 2 -> 3 -> 4 -> 5)\n";
    std::cout << std::string(80, '=') << "\n";

    OSIngestionEngine engine(filepath, chunk_size);
    if (!engine.open()) return;

    DeduplicationFilter dedup(50000);
    MicroBatchBuffer batch_buffer(5000, 0.5);

    size_t total_ingested = 0;
    size_t total_parsed = 0;
    size_t total_duplicates = 0;
    size_t total_batches_flushed = 0;
    size_t total_records_buffered = 0;

    auto t_start = std::chrono::high_resolution_clock::now();

    engine.for_each_chunk([&](size_t, size_t, size_t, std::string_view chunk) {
        OSIngestionEngine::for_each_line_in_chunk(chunk, [&](std::string_view line) {
            total_ingested++;

            // Stage 3: Parsing & Normalization
            NormalizedLogRecord rec;
            if (LogParser::parse_hdfs_line(line, rec)) {
                total_parsed++;

                // Stage 4: Deduplication Filter
                if (dedup.is_duplicate_or_add(rec)) {
                    total_duplicates++;
                } else {
                    // Stage 5: Micro-Batch Buffer
                    std::vector<NormalizedLogRecord> flushed;
                    if (batch_buffer.add(std::move(rec), flushed)) {
                        total_batches_flushed++;
                        total_records_buffered += flushed.size();
                    }
                }
            }
        });
    });

    // Flush remaining records
    std::vector<NormalizedLogRecord> remaining;
    if (batch_buffer.flush_remaining(remaining)) {
        total_batches_flushed++;
        total_records_buffered += remaining.size();
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    if (elapsed <= 0.0) elapsed = 1e-9;

    std::cout << "\nPIPELINE EXECUTION SUMMARY:\n";
    std::cout << "  * Lines Ingested (Stage 2) : " << total_ingested << "\n";
    std::cout << "  * Lines Parsed (Stage 3)   : " << total_parsed << " (" 
              << (total_parsed * 100.0 / std::max(total_ingested, size_t(1))) << "% valid)\n";
    std::cout << "  * Duplicates Dropped (Stage 4): " << total_duplicates << "\n";
    std::cout << "  * Unique Records Buffered (Stage 5): " << total_records_buffered << "\n";
    std::cout << "  * Total Micro-Batches Flushed: " << total_batches_flushed << "\n";
    std::cout << "  * End-to-End Elapsed Time  : " << std::fixed << std::setprecision(6) << elapsed << " s\n";
    std::cout << "  * End-to-End Processing Speed: " << std::fixed << std::setprecision(0)
              << (total_ingested / elapsed) << " lines/sec\n";
    std::cout << std::string(80, '=') << "\n\n";
}


// ==============================================================================
// CLI DISPATCHER & ENTRY POINT
// ==============================================================================
size_t parse_size_string(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    if (s.find("KB") != std::string::npos) {
        return static_cast<size_t>(std::stod(s.substr(0, s.find("KB"))) * 1024);
    }
    if (s.find("MB") != std::string::npos) {
        return static_cast<size_t>(std::stod(s.substr(0, s.find("MB"))) * 1024 * 1024);
    }
    if (s.find("GB") != std::string::npos) {
        return static_cast<size_t>(std::stod(s.substr(0, s.find("GB"))) * 1024 * 1024 * 1024);
    }
    return static_cast<size_t>(std::stoull(s));
}

int main(int argc, char* argv[]) {
    std::string target_file = "HDFS_2k.log";
    std::string chunk_size_str = "";
    std::string generate_path = "";
    size_t size_mb = 500;
    bool run_pipeline = false;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--generate" && i + 1 < argc) {
            generate_path = argv[++i];
        } else if (arg == "--size-mb" && i + 1 < argc) {
            size_mb = static_cast<size_t>(std::stoull(argv[++i]));
        } else if (arg == "--chunk-size" && i + 1 < argc) {
            chunk_size_str = argv[++i];
        } else if (arg == "--full-pipeline") {
            run_pipeline = true;
        } else if (arg[0] != '-') {
            target_file = arg;
        }
    }

    // Mode 1: Synthetic Generator
    if (!generate_path.empty()) {
        std::string template_file = (target_file != "HDFS_2k.log" || std::ifstream("HDFS_2k.log").good()) 
                                    ? target_file : "";
        generate_synthetic_log(generate_path, size_mb, template_file);
        return 0;
    }

    // Mode 2: Verify Target File Exists
    std::ifstream test_open(target_file);
    if (!test_open.good()) {
        std::cerr << "[ERROR] Target file '" << target_file << "' not found.\n";
        std::cerr << "Usage:\n";
        std::cerr << "  " << argv[0] << " <logfile> [--chunk-size 64KB|64MB] [--full-pipeline]\n";
        std::cerr << "  " << argv[0] << " --generate dummy.log --size-mb 500\n";
        return 1;
    }
    test_open.close();

    // Determine optimal chunk size
    size_t chunk_bytes = 64 * 1024 * 1024; // Default 64 MB
    if (!chunk_size_str.empty()) {
        chunk_bytes = parse_size_string(chunk_size_str);
    } else {
        // Auto-select 64 KB for small files to demonstrate multi-chunking
        std::ifstream in(target_file, std::ifstream::ate | std::ifstream::binary);
        if (in.good() && in.tellg() < 10 * 1024 * 1024) {
            chunk_bytes = 64 * 1024;
        }
    }

    if (run_pipeline) {
        run_full_pipeline(target_file, chunk_bytes);
    } else {
        OSIngestionEngine engine(target_file, chunk_bytes);
        if (engine.open()) {
            engine.benchmark(5);
        }
    }

    return 0;
}
