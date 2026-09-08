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

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/tablet.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

using namespace common;
using namespace storage;
using Clock = std::chrono::steady_clock;
constexpr uint32_t kDevices = 10;
constexpr uint32_t kTabletRows = 100000;
constexpr uint32_t kDeviceRows = kTabletRows / kDevices;
constexpr int64_t kEpoch = 1700000000000LL;
const std::vector<std::string> kNames = {"device", "i32", "i64",
                                         "f32",    "f64", "i64_2"};
const std::vector<TSDataType> kTypes = {STRING, INT32,  INT64,
                                        FLOAT,  DOUBLE, INT64};
const std::vector<ColumnCategory> kCategories = {
    ColumnCategory::TAG,   ColumnCategory::FIELD, ColumnCategory::FIELD,
    ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD};

double seconds(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double>(end - start).count();
}
void check(int ret, const char* operation) {
    if (ret != E_OK)
        throw std::runtime_error(std::string(operation) + " returned " +
                                 std::to_string(ret));
}
void require(bool b, const std::string& message) {
    if (!b) throw std::runtime_error(message);
}
uint64_t mix(uint64_t x) {
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}
struct Values {
    int32_t i32;
    int64_t i64;
    float f32;
    double f64;
    int64_t i64_2;
};
Values values(uint32_t device, uint64_t row) {
    const uint64_t a = mix(row + UINT64_C(0x9e3779b97f4a7c15) * (device + 1));
    const uint64_t b = mix(a + UINT64_C(0x123456789abcdef));
    return {static_cast<int32_t>(a % 2000001) - 1000000,
            INT64_C(1000000000000) + static_cast<int64_t>(row * 100 + device),
            static_cast<float>(static_cast<int32_t>((a >> 32) & 0xffffff) -
                               0x800000) /
                256.0f,
            static_cast<double>(b >> 12) / 1048576.0,
            static_cast<int64_t>(b & UINT64_C(0x7fffffffffffffff))};
}
struct Options {
    std::string output;
    std::string codec = "plain_none";
    uint64_t rows = 200000000;
    int threads = 15;
    uint32_t page_rows = 10000;
    uint64_t chunk_mib = 512;
    bool verify_all = false;
    bool verify_only = false;
};
Options parse(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        std::string flag = argv[i];
        if (flag == "--verify-only") {
            o.verify_only = true;
            continue;
        }
        if (flag == "--verify-all") {
            o.verify_all = true;
            continue;
        }
        require(i + 1 < argc, "missing option value");
        std::string value = argv[++i];
        if (flag == "--output")
            o.output = value;
        else if (flag == "--codec")
            o.codec = value;
        else if (flag == "--rows-per-device")
            o.rows = std::stoull(value);
        else if (flag == "--threads")
            o.threads = std::stoi(value);
        else if (flag == "--page-rows")
            o.page_rows = std::stoul(value);
        else if (flag == "--chunk-mib")
            o.chunk_mib = std::stoull(value);
        else
            throw std::runtime_error("unknown option " + flag);
    }
    require(!o.output.empty(), "--output required");
    require(o.rows > 0 && o.rows % kDeviceRows == 0,
            "rows must be a positive multiple of 10000");
    require(o.rows <= 200000000 && o.threads >= 0 && o.threads <= 64,
            "rows/threads outside benchmark bounds");
    require(o.page_rows > 0 && o.page_rows <= 1000000 && o.chunk_mib > 0 &&
                o.chunk_mib <= 2048,
            "page/chunk bounds");
    require(o.codec == "plain_none" || o.codec == "plain_lz4" ||
                o.codec == "default_lz4",
            "unknown codec");
    require(o.verify_only || access(o.output.c_str(), F_OK) != 0,
            "output already exists; refusing to overwrite");
    return o;
}
uint64_t available_bytes(const std::string& directory) {
    struct statvfs s;
    require(statvfs(directory.c_str(), &s) == 0, "statvfs failed");
    return static_cast<uint64_t>(s.f_bavail) * s.f_frsize;
}
uint64_t file_bytes(const std::string& file) {
    struct stat s;
    return stat(file.c_str(), &s) == 0 ? static_cast<uint64_t>(s.st_size) : 0;
}
uint64_t verify(const Options& o) {
    TsFileReader reader;
    check(reader.open(o.output), "reader.open");
    auto devices = reader.get_all_devices("benchmark");
    require(devices.size() == kDevices, "wrong device count");
    auto metadata = reader.get_timeseries_metadata(devices);
    require(metadata.size() == kDevices, "missing device metadata");
    uint64_t series_points = 0;
    for (const auto& device : metadata) {
        std::set<std::string> fields;
        for (const auto& index : device.second) {
            const std::string name =
                index->get_measurement_name().to_std_string();
            if (name.empty()) continue;
            auto it = std::find(kNames.begin() + 1, kNames.end(), name);
            require(it != kNames.end() && fields.insert(name).second,
                    "unexpected metadata field");
            const auto* stat = index->get_statistic();
            require(stat != nullptr && stat->count_ == o.rows,
                    "wrong per-device series count");
            require(stat->start_time_ == kEpoch &&
                        stat->end_time_ == kEpoch + o.rows - 1,
                    "wrong series time range");
            require(index->get_data_type() == kTypes[it - kNames.begin()],
                    "wrong metadata type");
            series_points += stat->count_;
        }
        require(fields.size() == 5, "expected five numeric fields per device");
    }
    require(series_points == o.rows * kDevices * 5,
            "wrong total numeric point count");
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    if (o.verify_all)
        ranges.emplace_back(0, o.rows - 1);
    else {
        ranges.emplace_back(0, 2);
        ranges.emplace_back(kDeviceRows - 2,
                            std::min<uint64_t>(o.rows - 1, kDeviceRows + 2));
        ranges.emplace_back(o.rows / 2, o.rows / 2 + 2);
        ranges.emplace_back(o.rows - kDeviceRows, o.rows - 1);
    }
    uint64_t checked = 0;
    for (const auto& range : ranges) {
        ResultSet* result = nullptr;
        check(reader.query("benchmark", kNames, kEpoch + range.first,
                           kEpoch + range.second, result),
              "reader.query");
        std::vector<uint64_t> next_row(kDevices, range.first);
        bool next = false;
        for (;;) {
            check(result->next(next), "result.next");
            if (!next) break;
            for (uint32_t c = 1; c <= 7; ++c)
                require(!result->is_null(c), "unexpected NULL");
            const auto* tag = result->get_value<String*>(2);
            const std::string device(tag->buf_, tag->len_);
            require(device.size() == 9 && device.compare(0, 8, "device_0") == 0,
                    "incorrect device tag");
            const uint32_t d = static_cast<uint32_t>(device[8] - '0');
            require(d < kDevices, "incorrect device ordinal");
            const uint64_t row = result->get_value<int64_t>(1) - kEpoch;
            require(row == next_row[d]++ && row <= range.second,
                    "wrong sampled time/order");
            const Values v = values(d, row);
            if (!(result->get_value<int32_t>(3) == v.i32 &&
                  result->get_value<int64_t>(4) == v.i64 &&
                  result->get_value<float>(5) == v.f32 &&
                  result->get_value<double>(6) == v.f64 &&
                  result->get_value<int64_t>(7) == v.i64_2)) {
                std::cerr << std::setprecision(17)
                          << "MISMATCH device=" << device << " row=" << row
                          << " actual=" << result->get_value<int32_t>(3) << ","
                          << result->get_value<int64_t>(4) << ","
                          << result->get_value<float>(5) << ","
                          << result->get_value<double>(6) << ","
                          << result->get_value<int64_t>(7)
                          << " expected=" << v.i32 << "," << v.i64 << ","
                          << v.f32 << "," << v.f64 << "," << v.i64_2
                          << std::endl;
                throw std::runtime_error("sampled value mismatch");
            }
            ++checked;
        }
        reader.destroy_query_data_set(result);
        for (auto row : next_row)
            require(row == range.second + 1, "missing sampled rows");
    }
    return checked;
}
int main(int argc, char** argv) {
    try {
        const Options o = parse(argc, argv);
        const auto slash = o.output.find_last_of('/');
        const std::string directory =
            slash == std::string::npos ? "." : o.output.substr(0, slash);
        require(available_bytes(directory) >= 40ULL * 1024 * 1024 * 1024,
                "less than 40 GiB free");
        const auto start = Clock::now();
        struct rusage before;
        getrusage(RUSAGE_SELF, &before);
        check(set_thread_count(std::max(1, o.threads)), "set_thread_count");
        check(libtsfile_init(), "libtsfile_init");
        g_config_value_.parallel_write_enabled_ = o.threads != 0;
        g_config_value_.parallel_read_enabled_ = o.threads != 0;
        g_config_value_.page_writer_max_point_num_ = o.page_rows;
        g_config_value_.page_writer_max_memory_bytes_ = 16 * 1024 * 1024;
        g_config_value_.chunk_group_size_threshold_ = o.chunk_mib * 1024 * 1024;
        g_config_value_.record_count_for_next_mem_check_ = kTabletRows;
        g_config_value_.time_encoding_type_ = TS_2DIFF;
        g_config_value_.time_compress_type_ =
            o.codec == "plain_none" ? UNCOMPRESSED : LZ4;
        g_config_value_.sync_on_close_ = true;
        if (o.verify_only) {
            std::cout << "VERIFIED_ROWS " << verify(o) << std::endl;
            return 0;
        }
        double writer_setup_s = seconds(start, Clock::now());
        const auto prepare_setup_start = Clock::now();
        double prepare_s = 0, write_s = 0;
        std::vector<int64_t> timestamps(kTabletRows), i64(kTabletRows),
            i64_2(kTabletRows);
        std::vector<int32_t> i32(kTabletRows), offsets(kTabletRows + 1);
        std::vector<float> f32(kTabletRows);
        std::vector<double> f64(kTabletRows);
        std::vector<char> tags(kTabletRows * 9);
        for (uint32_t d = 0; d < kDevices; ++d) {
            const std::string tag = "device_0" + std::to_string(d);
            for (uint32_t r = 0; r < kDeviceRows; ++r) {
                const uint32_t pos = d * kDeviceRows + r;
                offsets[pos] = pos * 9;
                std::copy(tag.begin(), tag.end(), tags.begin() + pos * 9);
            }
        }
        offsets[kTabletRows] = tags.size();
        Tablet tablet("benchmark", kNames, kTypes, kCategories, kTabletRows);
        check(tablet.err_code_, "Tablet.init");
        const auto writer_setup_start = Clock::now();
        const double prepare_setup_s =
            seconds(prepare_setup_start, writer_setup_start);
        prepare_s += prepare_setup_s;
        TsFileWriter writer;
        check(writer.open(o.output), "writer.open");
        std::vector<ColumnSchema> columns;
        for (uint32_t c = 0; c < kNames.size(); ++c) {
            const TSEncoding encoding =
                c == 0 || o.codec != "default_lz4"
                    ? PLAIN
                    : (kTypes[c] == FLOAT || kTypes[c] == DOUBLE ? GORILLA
                                                                 : TS_2DIFF);
            columns.emplace_back(kNames[c], kTypes[c],
                                 o.codec == "plain_none" ? UNCOMPRESSED : LZ4,
                                 encoding, kCategories[c]);
        }
        check(writer.register_table(
                  std::make_shared<TableSchema>("benchmark", columns)),
              "writer.register_table");
        const auto setup_end = Clock::now();
        writer_setup_s += seconds(writer_setup_start, setup_end);
        auto progress_at = start;
        uint64_t tablets = 0;
        std::cout << std::setprecision(12)
                  << "{\"event\":\"start\",\"rows_per_device\":" << o.rows
                  << ",\"devices\":10,\"tablet_rows\":100000,\"codec\":\""
                  << o.codec << "\",\"threads\":" << o.threads
                  << ",\"page_rows\":" << o.page_rows
                  << ",\"chunk_mib\":" << o.chunk_mib << "}" << std::endl;
        for (uint64_t base = 0; base < o.rows; base += kDeviceRows) {
            const auto prep_start = Clock::now();
            tablet.reset();
            // Generate contiguous column arrays, device 0 then device 1, ...
            // All ingestion below is by bulk-column APIs, never
            // add_value/add_timestamp.
            for (uint32_t d = 0; d < kDevices; ++d) {
                for (uint32_t r = 0; r < kDeviceRows; ++r) {
                    const uint32_t pos = d * kDeviceRows + r;
                    const Values v = values(d, base + r);
                    timestamps[pos] = kEpoch + base + r;
                    i32[pos] = v.i32;
                    i64[pos] = v.i64;
                    f32[pos] = v.f32;
                    f64[pos] = v.f64;
                    i64_2[pos] = v.i64_2;
                }
            }
            check(tablet.set_timestamps(timestamps.data(), kTabletRows),
                  "bulk timestamps");
            check(tablet.set_column_string_values(
                      0, offsets.data(), tags.data(), nullptr, kTabletRows),
                  "bulk tags");
            check(tablet.set_column_values(1, i32.data(), nullptr, kTabletRows),
                  "bulk i32");
            check(tablet.set_column_values(2, i64.data(), nullptr, kTabletRows),
                  "bulk i64");
            check(tablet.set_column_values(3, f32.data(), nullptr, kTabletRows),
                  "bulk f32");
            check(tablet.set_column_values(4, f64.data(), nullptr, kTabletRows),
                  "bulk f64");
            check(
                tablet.set_column_values(5, i64_2.data(), nullptr, kTabletRows),
                "bulk i64_2");
            const auto prep_end = Clock::now();
            prepare_s += seconds(prep_start, prep_end);
            check(writer.write_table(tablet), "writer.write_table");
            const auto written = Clock::now();
            write_s += seconds(prep_end, written);
            ++tablets;
            if (seconds(progress_at, written) >= 5) {
                require(
                    available_bytes(directory) >= 40ULL * 1024 * 1024 * 1024,
                    "free disk space fell below 40 GiB");
                std::cout << "{\"event\":\"progress\",\"rows\":"
                          << tablets * kTabletRows
                          << ",\"elapsed_s\":" << seconds(start, written)
                          << ",\"file_bytes\":" << file_bytes(o.output) << "}"
                          << std::endl;
                progress_at = written;
            }
        }
        const auto flush_start = Clock::now();
        check(writer.flush(), "writer.flush");
        const auto close_start = Clock::now();
        check(writer.close(), "writer.close (including fsync)");
        const auto closed = Clock::now();
        // On macOS, also request the drive-cache barrier explicitly and time
        // it.
        int durable_fd = open(o.output.c_str(), O_RDWR);
        require(durable_fd >= 0, "open for durable sync failed");
#ifdef __APPLE__
        require(fcntl(durable_fd, F_FULLFSYNC) == 0, "F_FULLFSYNC failed");
#else
        require(fsync(durable_fd) == 0, "fsync failed");
#endif
        require(close(durable_fd) == 0, "durable fd close failed");
        const auto durable = Clock::now();
        struct rusage after;
        getrusage(RUSAGE_SELF, &after);
        const double wall = seconds(start, durable);
        const double flush = seconds(flush_start, close_start);
        const double close_time = seconds(close_start, closed);
        const double barrier = seconds(closed, durable);
        const double cpu_user =
            after.ru_utime.tv_sec - before.ru_utime.tv_sec +
            (after.ru_utime.tv_usec - before.ru_utime.tv_usec) / 1000000.0;
        const double cpu_sys =
            after.ru_stime.tv_sec - before.ru_stime.tv_sec +
            (after.ru_stime.tv_usec - before.ru_stime.tv_usec) / 1000000.0;
        const uint64_t rows = o.rows * kDevices;
        const double ingestion_s =
            writer_setup_s + write_s + flush + close_time + barrier;
        const double other_s = wall - prepare_s - ingestion_s;
#ifdef __APPLE__
        const uint64_t peak_rss_bytes = after.ru_maxrss;
#else
        const uint64_t peak_rss_bytes =
            static_cast<uint64_t>(after.ru_maxrss) * 1024;
#endif
        std::cout << "{\"event\":\"write_complete\",\"end_to_end_s\":" << wall
                  << ",\"rows\":" << rows
                  << ",\"setup_s\":" << seconds(start, setup_end)
                  << ",\"prepare_s\":" << prepare_s
                  << ",\"prepare_setup_s\":" << prepare_setup_s
                  << ",\"writer_setup_s\":" << writer_setup_s
                  << ",\"ingestion_s\":" << ingestion_s
                  << ",\"other_s\":" << other_s << ",\"parallel_write\":"
                  << (o.threads != 0 ? "true" : "false")
                  << ",\"writer_threads\":" << (o.threads == 0 ? 1 : o.threads)
                  << ",\"write_rows_per_s\":" << rows / ingestion_s
                  << ",\"write_numeric_points_per_s\":"
                  << rows * 5 / ingestion_s << ",\"write_calls_s\":" << write_s
                  << ",\"final_flush_s\":" << flush
                  << ",\"close_fsync_s\":" << close_time
                  << ",\"full_sync_s\":" << barrier
                  << ",\"cpu_user_s\":" << cpu_user
                  << ",\"cpu_system_s\":" << cpu_sys
                  << ",\"peak_rss_bytes\":" << peak_rss_bytes
                  << ",\"file_bytes\":" << file_bytes(o.output) << "}"
                  << std::endl;
        const auto verify_start = Clock::now();
        const uint64_t verified_rows = verify(o);
        const double verify_s = seconds(verify_start, Clock::now());
        std::cout << "{\"event\":\"result\",\"rows\":" << rows
                  << ",\"numeric_points\":" << rows * 5
                  << ",\"tablets\":" << tablets << ",\"end_to_end_s\":" << wall
                  << ",\"setup_s\":" << seconds(start, setup_end)
                  << ",\"prepare_s\":" << prepare_s
                  << ",\"prepare_setup_s\":" << prepare_setup_s
                  << ",\"writer_setup_s\":" << writer_setup_s
                  << ",\"ingestion_s\":" << ingestion_s
                  << ",\"other_s\":" << other_s << ",\"parallel_write\":"
                  << (o.threads != 0 ? "true" : "false")
                  << ",\"writer_threads\":" << (o.threads == 0 ? 1 : o.threads)
                  << ",\"write_rows_per_s\":" << rows / ingestion_s
                  << ",\"write_numeric_points_per_s\":"
                  << rows * 5 / ingestion_s << ",\"write_calls_s\":" << write_s
                  << ",\"final_flush_s\":" << flush
                  << ",\"close_fsync_s\":" << close_time
                  << ",\"full_sync_s\":" << barrier
                  << ",\"write_flush_close_s\":"
                  << write_s + flush + close_time + barrier
                  << ",\"rows_per_s\":" << rows / wall
                  << ",\"numeric_points_per_s\":" << rows * 5 / wall
                  << ",\"file_bytes\":" << file_bytes(o.output)
                  << ",\"cpu_user_s\":" << cpu_user
                  << ",\"cpu_system_s\":" << cpu_sys
                  << ",\"peak_rss_bytes\":" << peak_rss_bytes
                  << ",\"verification_s\":" << verify_s
                  << ",\"verified_rows\":" << verified_rows
                  << ",\"metadata_numeric_points\":" << rows * 5
                  << ",\"verification\":\"passed\"}" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "BENCHMARK FAILED: " << e.what() << std::endl;
        return 1;
    }
}
