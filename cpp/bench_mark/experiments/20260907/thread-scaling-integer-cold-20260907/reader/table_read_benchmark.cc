/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/thread_pool.h"
#include "common/tsblock/tsblock.h"
#include "reader/tsfile_reader.h"

#if !defined(ENABLE_THREADS) || !defined(ENABLE_SIMD) || !defined(NDEBUG)
#error "Benchmark requires Release, ENABLE_THREADS and ENABLE_SIMD"
#endif

using namespace common;
using namespace storage;
using Clock = std::chrono::steady_clock;
constexpr uint32_t kDevices = 10;
constexpr int64_t kEpoch = INT64_C(1700000000000);
const std::vector<std::string> kColumns = {"device", "i32",   "i64",
                                           "i32_2",  "i64_2", "i64_3"};
const std::array<TSDataType, 7> kTypes = {INT64, STRING, INT32, INT64,
                                          INT32, INT64,  INT64};
const std::array<uint32_t, 7> kWidths = {8, 13, 4, 8, 4, 8, 8};

void require(bool value, const std::string& message) {
    if (!value) throw std::runtime_error(message);
}
void check(int status, const char* operation) {
    require(status == E_OK,
            std::string(operation) + " returned " + std::to_string(status));
}
double seconds(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}
double cpu_seconds(const timeval& t) {
    return t.tv_sec + t.tv_usec / 1000000.0;
}
uint64_t mix(uint64_t x) {
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}
template <typename T>
T load_value(TsBlock* block, uint32_t col, uint32_t row) {
    T value;
    std::memcpy(
        &value,
        block->get_vector(col)->get_value_data().get_data() + sizeof(T) * row,
        sizeof(T));
    return value;
}
uint32_t tag_device(TsBlock* block, uint32_t row) {
    // This fixture has exactly nine bytes per TAG. The buffer-size check
    // precedes access; each sampled entry also verifies its length prefix.
    const char* p =
        block->get_vector(1)->get_value_data().get_data() + 13 * row;
    uint32_t len;
    std::memcpy(&len, p, sizeof(len));
    require(len == 9 && std::memcmp(p + 4, "device_0", 8) == 0 &&
                p[12] >= '0' && p[12] <= '9',
            "invalid device tag");
    return static_cast<uint32_t>(p[12] - '0');
}

struct Options {
    std::string file;
    uint64_t rows_per_device = 200000000;
    int threads = 15;
    int batch_size = 100000;
    bool verify_all = false;
};
Options parse(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string flag = argv[i];
        if (flag == "--verify-all") {
            o.verify_all = true;
            continue;
        }
        require(i + 1 < argc, "missing option value");
        const std::string value = argv[++i];
        if (flag == "--file")
            o.file = value;
        else if (flag == "--threads")
            o.threads = std::stoi(value);
        else if (flag == "--batch-size")
            o.batch_size = std::stoi(value);
        else if (flag == "--rows-per-device")
            o.rows_per_device = std::stoull(value);
        else
            throw std::runtime_error("unknown argument " + flag);
    }
    require(!o.file.empty(), "--file required");
    require(o.threads >= 1 && o.threads <= 64, "require 1..64 worker threads");
    require(o.batch_size > 0 && o.batch_size <= 1000000, "invalid batch size");
    require(o.rows_per_device > 0 && o.rows_per_device <= 200000000,
            "invalid fixture row count");
    return o;
}

struct Validation {
    std::array<uint64_t, kDevices> device_rows{};
    uint64_t sampled_rows = 0;
    uint64_t checksum = 0;
    uint64_t returned_bytes = 0;
    void row(TsBlock* block, uint32_t index, uint32_t device, uint64_t number) {
        require(tag_device(block, index) == device, "mixed device block");
        require(load_value<int64_t>(block, 0, index) ==
                    kEpoch + static_cast<int64_t>(number),
                "wrong timestamp");
        const uint64_t a =
            mix(number + UINT64_C(0x9e3779b97f4a7c15) * (device + 1));
        const uint64_t b = mix(a + UINT64_C(0x123456789abcdef));
        require(load_value<int32_t>(block, 2, index) ==
                    static_cast<int32_t>(a % 2000001) - 1000000,
                "wrong i32");
        require(load_value<int64_t>(block, 3, index) ==
                    INT64_C(1000000000000) +
                        static_cast<int64_t>(number * 100 + device),
                "wrong i64");
        require(load_value<int32_t>(block, 4, index) ==
                    static_cast<int32_t>((a >> 32) & 0xffffff) - 0x800000,
                "wrong i32_2");
        require(load_value<int64_t>(block, 5, index) ==
                    static_cast<int64_t>(b >> 12),
                "wrong i64_2");
        const int64_t i64_3 = load_value<int64_t>(block, 6, index);
        require(i64_3 == static_cast<int64_t>(b & UINT64_C(0x7fffffffffffffff)),
                "wrong i64_3");
        checksum += mix(static_cast<uint64_t>(i64_3) ^ number ^
                        (uint64_t(device) << 56));
        ++sampled_rows;
    }
    void block(TsBlock* block, const Options& o) {
        require(block && block->get_column_count() == 7, "wrong block schema");
        const uint32_t n = block->get_row_count();
        require(n > 0 && n <= static_cast<uint32_t>(o.batch_size),
                "invalid block size");
        for (uint32_t c = 0; c < 7; ++c) {
            Vector* vector = block->get_vector(c);
            require(vector->get_vector_type() == kTypes[c],
                    "wrong column type");
            require(!vector->has_null(), "unexpected NULL in dense fixture");
            const uint64_t bytes = vector->get_value_data().get_data_size();
            require(bytes == uint64_t(n) * kWidths[c],
                    "incomplete column buffer");
            returned_bytes += bytes;
        }
        const uint32_t device = tag_device(block, 0);
        const uint64_t first = device_rows[device];
        require(first + n <= o.rows_per_device, "too many rows for device");
        if (o.verify_all) {
            for (uint32_t i = 0; i < n; ++i) row(block, i, device, first + i);
        } else {
            row(block, 0, device, first);
            if (n > 2) row(block, n / 2, device, first + n / 2);
            if (n > 1) row(block, n - 1, device, first + n - 1);
        }
        device_rows[device] += n;
    }
};

// Linux process I/O counter is recorded as telemetry, never used to skip data.
int64_t read_io_bytes() {
#ifdef __linux__
    std::ifstream in("/proc/self/io");
    std::string name;
    int64_t value;
    while (in >> name >> value)
        if (name == "read_bytes:") return value;
#endif
    return -1;
}

int main(int argc, char** argv) {
    try {
        const Options o = parse(argc, argv);
        struct stat file_stat;
        require(stat(o.file.c_str(), &file_stat) == 0, "stat input failed");
        const uint64_t file_bytes = file_stat.st_size;
        const int64_t io_before = read_io_bytes();
        struct rusage before, after;
        require(getrusage(RUSAGE_SELF, &before) == 0, "getrusage failed");
        const auto start = Clock::now();
        check(set_thread_count(o.threads), "set_thread_count");
        check(libtsfile_init(), "libtsfile_init");
        set_parallel_read_enabled(o.threads > 1);
        require(get_parallel_read_enabled() == (o.threads > 1),
                "parallel read mode not configured");
        require(g_thread_pool_ &&
                    g_thread_pool_->num_threads() == size_t(o.threads),
                "worker pool not configured");
        const auto initialized = Clock::now();
        std::unique_ptr<TsFileReader> reader(new TsFileReader());
        check(reader->open(o.file), "reader.open");
        const auto opened = Clock::now();
        ResultSet* result = nullptr;
        // A single complete table query, all TAG/FIELD columns, no tag/field
        // predicates, no row offset/limit, no device or time partitioning.
        check(reader->query(
                  "benchmark", kColumns, std::numeric_limits<int64_t>::min(),
                  std::numeric_limits<int64_t>::max(), result, o.batch_size),
              "reader.query");
        require(result != nullptr, "missing result set");
        const auto queried = Clock::now();
        std::cout << std::setprecision(12)
                  << "{\"event\":\"start\",\"pid\":" << getpid()
                  << ",\"threads\":" << o.threads
                  << ",\"batch_size\":" << o.batch_size
                  << ",\"time_min\":-9223372036854775808,\"time_max\":"
                     "9223372036854775807"
                  << ",\"file_bytes\":" << file_bytes
                  << ",\"parallel_read\":" << (o.threads > 1 ? "true" : "false")
                  << ",\"simd\":true,\"compiler\":\"" << __VERSION__ << "\"}"
                  << std::endl;
        uint64_t rows = 0, blocks = 0;
        uint32_t min_block = std::numeric_limits<uint32_t>::max(),
                 max_block = 0;
        double read_api_s = 0, validation_s = 0, first_block_s = 0;
        Validation validation;
        auto last_progress = queried;
        for (;;) {
            TsBlock* block = nullptr;
            const auto call_start = Clock::now();
            const int status = result->get_next_tsblock(block);
            const auto returned = Clock::now();
            read_api_s += seconds(call_start, returned);
            if (status == E_NO_MORE_DATA) break;
            check(status, "get_next_tsblock");
            if (blocks == 0) first_block_s = seconds(start, returned);
            validation.block(block, o);
            const uint32_t n = block->get_row_count();
            rows += n;
            ++blocks;
            min_block = std::min(min_block, n);
            max_block = std::max(max_block, n);
            const auto validated = Clock::now();
            validation_s += seconds(returned, validated);
            if (seconds(last_progress, validated) >= 10) {
                std::cout << "{\"event\":\"progress\",\"rows\":" << rows
                          << ",\"blocks\":" << blocks
                          << ",\"elapsed_s\":" << seconds(start, validated)
                          << ",\"read_api_s\":" << read_api_s << "}"
                          << std::endl;
                last_progress = validated;
            }
            // The TsBlock is borrowed from the reader and consumed here;
            // it must not be freed or retained across the next API call.
        }
        require(rows == o.rows_per_device * kDevices, "wrong total row count");
        for (uint64_t n : validation.device_rows)
            require(n == o.rows_per_device, "wrong per-device row count");
        const auto close_start = Clock::now();
        reader->destroy_query_data_set(result);
        check(reader->close(), "reader.close");
        reader.reset();
        libtsfile_destroy();
        const auto end = Clock::now();
        require(getrusage(RUSAGE_SELF, &after) == 0, "getrusage failed");
        const int64_t io_after = read_io_bytes();
        const double init_s = seconds(start, initialized);
        const double open_s = seconds(initialized, opened);
        const double query_s = seconds(opened, queried);
        const double close_s = seconds(close_start, end);
        const double e2e_s = seconds(start, end);
        const double read_s = init_s + open_s + query_s + read_api_s + close_s;
        const double cpu_user =
            cpu_seconds(after.ru_utime) - cpu_seconds(before.ru_utime);
        const double cpu_sys =
            cpu_seconds(after.ru_stime) - cpu_seconds(before.ru_stime);
        uint64_t peak_rss = after.ru_maxrss;
#ifndef __APPLE__
        peak_rss *= 1024;
#endif
        std::cout
            << "{\"event\":\"result\",\"rows\":" << rows
            << ",\"numeric_points\":" << rows * 5 << ",\"blocks\":" << blocks
            << ",\"min_block_rows\":" << min_block
            << ",\"max_block_rows\":" << max_block
            << ",\"threads\":" << o.threads
            << ",\"parallel_read\":" << (o.threads > 1 ? "true" : "false")
            << ",\"batch_size\":" << o.batch_size << ",\"init_s\":" << init_s
            << ",\"open_s\":" << open_s << ",\"query_s\":" << query_s
            << ",\"first_block_from_start_s\":" << first_block_s
            << ",\"read_api_s\":" << read_api_s
            << ",\"validation_s\":" << validation_s
            << ",\"close_s\":" << close_s << ",\"read_total_s\":" << read_s
            << ",\"end_to_end_s\":" << e2e_s
            << ",\"other_s\":" << e2e_s - read_s - validation_s
            << ",\"read_rows_per_s\":" << rows / read_s
            << ",\"read_numeric_points_per_s\":" << rows * 5 / read_s
            << ",\"end_to_end_rows_per_s\":" << rows / e2e_s
            << ",\"cpu_user_s\":" << cpu_user << ",\"cpu_system_s\":" << cpu_sys
            << ",\"average_cpu_cores\":" << (cpu_user + cpu_sys) / e2e_s
            << ",\"peak_rss_bytes\":" << peak_rss
            << ",\"file_bytes\":" << file_bytes
            << ",\"returned_column_buffer_bytes\":" << validation.returned_bytes
            << ",\"process_read_io_bytes\":"
            << (io_before >= 0 && io_after >= io_before ? io_after - io_before
                                                        : -1)
            << ",\"input_blocks\":" << after.ru_inblock - before.ru_inblock
            << ",\"sampled_rows\":" << validation.sampled_rows
            << ",\"sample_checksum\":\"" << validation.checksum << "\""
            << ",\"verification\":\"passed\",\"verify_all\":"
            << (o.verify_all ? "true" : "false") << ",\"device_rows\":[";
        for (size_t i = 0; i < kDevices; ++i) {
            if (i) std::cout << ',';
            std::cout << validation.device_rows[i];
        }
        std::cout << "]}" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "READ BENCHMARK FAILED: " << e.what() << std::endl;
        return 1;
    }
}
