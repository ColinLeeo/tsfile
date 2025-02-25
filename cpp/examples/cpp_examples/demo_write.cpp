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

#include <cwrapper/tsfile_cwrapper.h>
#include <time.h>
#include <writer/tsfile_table_writer.h>

#include <iostream>
#include <random>
#include <string>

#include "cpp_examples.h"

using namespace storage;

long getNowTime() { return time(nullptr); }

int demo_write() {
    std::string table_name = "table1";
    WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    file.create("test.tsfile", flags, mode);

    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    storage::TableSchema* schema = new storage::TableSchema(
        table_name, {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED, common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("id2", common::STRING,  common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT32),
        }
        );
    int id_schema_num = 5;
    int measurement_schema_num = 5;
    for (int i = 0; i < id_schema_num; i++) {
        measurement_schemas.emplace_back(new MeasurementSchema(
            "id" + std::to_string(i), TSDataType::STRING, TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED));
        column_categories.emplace_back(ColumnCategory::TAG);
    }
    for (int i = 0; i < measurement_schema_num; i++) {
        measurement_schemas.emplace_back(new MeasurementSchema(
            "s" + to_string(i), TSDataType::INT64, TSEncoding::PLAIN,
            CompressionType::UNCOMPRESSED));
        column_categories.emplace_back(ColumnCategory::FIELD);
    }

    TsFileTableWriter writer = new TsFileTableWriter(file, )

}
