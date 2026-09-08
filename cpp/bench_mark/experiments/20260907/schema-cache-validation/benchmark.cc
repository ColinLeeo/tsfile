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
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include "writer/tsfile_writer.h"
#include "common/config/config.h"
using namespace storage;
using namespace common;
static void check(int ret) { if (ret != E_OK) { std::cerr << "error " << ret << '\n'; std::exit(1); } }
int main(int argc, char** argv) {
    const std::string prefix = argc > 1 ? argv[1] : "/private/tmp/pr934-bench";
    libtsfile_init();
    g_config_value_.parallel_write_enabled_ = false;
    g_config_value_.chunk_group_size_threshold_ = INT64_MAX;
    g_config_value_.record_count_for_next_mem_check_ = INT32_MAX;
    g_config_value_.time_compress_type_ = UNCOMPRESSED;
    for (int mode = 0; mode < 6; ++mode) {
        std::vector<double> timings;
        for (int round = 0; round < 5; ++round) {
            std::string filename = prefix + "-" + std::to_string(mode) + ".tsfile";
            std::remove(filename.c_str());
            TsFileWriter writer;
            check(writer.open(filename));
            std::vector<std::string> names;
            std::vector<TSDataType> types;
            std::vector<ColumnSchema> columns;
            for (int c = 0; c < 200; ++c) {
                names.push_back("s" + std::to_string(c));
                types.push_back(INT32);
                MeasurementSchema ms(names.back(), INT32, PLAIN, UNCOMPRESSED);
                if (mode == 0 || mode == 3 || mode == 4) check(writer.register_timeseries("root.bench", ms));
                if (mode == 4) check(writer.register_timeseries("root.bench2", ms));
                if (mode == 1) check(writer.register_aligned_timeseries("root.bench", ms));
                columns.emplace_back(names.back(), INT32, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD);
            }
            if (mode == 2 || mode == 5) check(writer.register_table(std::make_shared<TableSchema>("bench", columns)));
            if (mode == 5) check(writer.register_table(std::make_shared<TableSchema>("bench2", columns)));
            Tablet full(mode == 2 || mode == 5 ? "bench" : "root.bench", &names, &types, 1);
            Tablet other(mode == 5 ? "bench2" : "root.bench2", &names, &types, 1);
            for (uint32_t c = 0; c < 200; ++c) check(other.add_value(0, c, int32_t(c)));
            names.pop_back(); types.pop_back();
            Tablet narrow("root.bench", &names, &types, 1);
            for (uint32_t c = 0; c < 200; ++c) check(full.add_value(0, c, int32_t(c)));
            for (uint32_t c = 0; c < 199; ++c) check(narrow.add_value(0, c, int32_t(c)));
            const int writes = 5000;
            auto start = std::chrono::steady_clock::now();
            for (int w = 0; w < writes; ++w) {
                auto& tablet = mode >= 4 && w % 2 ? other : mode == 3 && w % 2 ? narrow : full;
                check(tablet.add_timestamp(0, w));
                check(mode == 2 || mode == 5 ? writer.write_table(tablet) : writer.write_tree(tablet));
            }
            auto elapsed = std::chrono::steady_clock::now() - start;
            timings.push_back(std::chrono::duration<double, std::micro>(elapsed).count() / writes);
            check(writer.flush());
            check(writer.close());
        }
        std::sort(timings.begin(), timings.end());
        std::cout << "mode=" << mode << " median_us=" << timings[2] << " min_us=" << timings[0] << '\n';
    }
    std::cout << "schema_group_bytes=" << sizeof(MeasurementSchemaGroup) << '\n';
    libtsfile_destroy();
}
