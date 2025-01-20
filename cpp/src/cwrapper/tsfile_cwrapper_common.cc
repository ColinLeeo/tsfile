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

#include "tsfile_cwrapper_common.h"

#include <iomanip>

#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"
#include "common/tablet.h"
#include "common/global.h"
#include "reader/expression.h"
static bool is_init = false;

Tablet tablet_new(const char* device_id, const char** column_name_list,
                  TSDataType* data_types, uint32_t column_num) {
    std::vector<std::string> measurement_list;
    std::vector<common::TSDataType> data_type_list;
    for (int i = 0; i < column_num; i++) {
        measurement_list.emplace_back(column_name_list[i]);
        data_type_list.push_back(
            static_cast<common::TSDataType>(*(data_types + i)));
    }
    auto* tablet =
        new storage::Tablet(device_id, &measurement_list, &data_type_list);
    return tablet;
}

uint32_t tablet_get_cur_row_size(Tablet tablet) {
    return static_cast<storage::Tablet*>(tablet)->get_cur_row_size();
}

void tablet_set_cur_row_size(Tablet tablet, uint32_t size) {
    static_cast<storage::Tablet*>(tablet)->set_row_size(size);
}

ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           timestamp timestamp) {
    return static_cast<storage::Tablet*>(tablet)->add_timestamp(row_index,
                                                                timestamp);
}

#define tablet_add_value_by_name_(type)                                      \
    ERRNO tablet_add_value_by_name_##type(Tablet tablet, uint32_t row_index, \
                                          const char* column_name,           \
                                          type value) {                      \
        return static_cast<storage::Tablet*>(tablet)->add_value(             \
            row_index, column_name, value);                                  \
    }

tablet_add_value_by_name_(int32_t);
tablet_add_value_by_name_(int64_t);
tablet_add_value_by_name_(float);
tablet_add_value_by_name_(double);
tablet_add_value_by_name_(bool);

#define table_add_value_by_index_(type)                                       \
    ERRNO tablet_add_value_by_index_##type(Tablet tablet, uint32_t row_index, \
                                           uint32_t column_index,             \
                                           type value) {                      \
        return static_cast<storage::Tablet*>(tablet)->add_value(              \
            row_index, column_index, value);                                  \
    }

table_add_value_by_index_(int32_t);
table_add_value_by_index_(int64_t);
table_add_value_by_index_(float);
table_add_value_by_index_(double);
table_add_value_by_index_(bool);

void* tablet_get_value(Tablet tablet, uint32_t row_index, uint32_t schema_index,
                       TSDataType *type) {
    return static_cast<storage::Tablet*>(tablet)->get_value(row_index, schema_index, type);
}


void init_tsfile_config() {
    if (!is_init) {
        common::init_config_value();
        is_init = true;
    }
}

TsFileReader tsfile_reader_open(const char* pathname, ERRNO* err_code) {
    init_tsfile_config();
    auto reader = new storage::TsFileReader();
    int ret = reader->open(pathname);
    if (ret != common::E_OK) {
        std::cout << "open file failed" << std::endl;
        *err_code = ret;
        delete reader;
        return nullptr;
    }
    return reader;
}

TsFileWriter tsfile_writer_open(const char* pathname, ERRNO* err_code) {
    init_tsfile_config();
    auto writer = new storage::TsFileWriter();
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    int ret = writer->open(pathname, flags, 0644);
    if (ret != common::E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

TsFileWriter tsfile_writer_open_flag(const char* pathname, mode_t flag,
                                  ERRNO* err_code) {
    init_tsfile_config();
    auto* writer = new storage::TsFileWriter();
    int ret = writer->open(pathname, O_CREAT | O_RDWR, flag);
    if (ret != common::E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

ERRNO tsfile_writer_close(TsFileWriter writer) {
    auto* w = (storage::TsFileWriter*)writer;
    int ret = w->close();
    delete w;
    return ret;
}

ERRNO tsfile_reader_close(TsFileReader reader) {
    auto* ts_reader = (storage::TsFileReader*)reader;
    delete ts_reader;
    return common::E_OK;
}
