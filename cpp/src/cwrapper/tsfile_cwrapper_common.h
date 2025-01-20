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

#include <iostream>

typedef enum {
    TS_DATATYPE_BOOLEAN = 0,
    TS_DATATYPE_INT32 = 1,
    TS_DATATYPE_INT64 = 2,
    TS_DATATYPE_FLOAT = 3,
    TS_DATATYPE_DOUBLE = 4,
    TS_DATATYPE_TEXT = 5,
    TS_DATATYPE_VECTOR = 6,
    TS_DATATYPE_NULL_TYPE = 254,
    TS_DATATYPE_INVALID = 255
} TSDataType;

typedef enum {
    TS_ENCODING_PLAIN = 0,
    TS_ENCODING_DICTIONARY = 1,
    TS_ENCODING_RLE = 2,
    TS_ENCODING_DIFF = 3,
    TS_ENCODING_TS_2DIFF = 4,
    TS_ENCODING_BITMAP = 5,
    TS_ENCODING_GORILLA_V1 = 6,
    TS_ENCODING_REGULAR = 7,
    TS_ENCODING_GORILLA = 8,
    TS_ENCODING_ZIGZAG = 9,
    TS_ENCODING_FREQ = 10,
    TS_ENCODING_INVALID = 255
} TSEncoding;

typedef enum {
    TS_COMPRESSION_UNCOMPRESSED = 0,
    TS_COMPRESSION_SNAPPY = 1,
    TS_COMPRESSION_GZIP = 2,
    TS_COMPRESSION_LZO = 3,
    TS_COMPRESSION_SDT = 4,
    TS_COMPRESSION_PAA = 5,
    TS_COMPRESSION_PLA = 6,
    TS_COMPRESSION_LZ4 = 7,
    TS_COMPRESSION_INVALID = 255
} CompressionType;

typedef enum column_category { TAG, FIELD, ATTRIBUTE } ColumnCategory;

typedef struct {
    char** column_names;
    TSDataType* data_types;
    uint32_t column_num;
}* ResultSetMetaData;

typedef struct tsfile_conf {
    int mem_threshold_kb;
} TsFileConf;

typedef void* TsFileReader;
typedef void* TsFileWriter;

// just resue Tablet from c++
typedef void* Tablet;

typedef void* ResultSet;

typedef int32_t ERRNO;
typedef int64_t timestamp;

// Tablet API
Tablet tablet_new(const char* device_id, const char** column_name_list,
                  TSDataType* data_types, uint32_t column_num);

uint32_t tablet_get_cur_row_size(Tablet tablet);

void tablet_set_cur_row_size(Tablet tablet, uint32_t size);

ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           timestamp timestamp);

#define tablet_add_value_by_name_(type)                                       \
    ERRNO tablet_add_value_by_name_##type(Tablet* tablet, uint32_t row_index, \
                                          char* column_name, type value);

tablet_add_value_by_name_(int32_t);
tablet_add_value_by_name_(int64_t);
tablet_add_value_by_name_(float);
tablet_add_value_by_name_(double);
tablet_add_value_by_name_(bool);

#define table_add_value_by_index_(type)                                        \
    ERRNO tablet_add_value_by_index_##type(Tablet* tablet, uint32_t row_index, \
                                           uint32_t column_index, type value);

table_add_value_by_index_(int32_t);
table_add_value_by_index_(int64_t);
table_add_value_by_index_(float);
table_add_value_by_index_(double);
table_add_value_by_index_(bool);

void* tablet_get_value(Tablet tablet, uint32_t row_index, uint32_t schema_index,
                       TSDataType &type);


TsFileReader tsfile_reader_open(const char* pathname, ERRNO* err_code);
TsFileWriter tsfile_writer_open(const char* pathname, ERRNO* err_code);
TsFileWriter tsfile_writer_open_flag(const char* pathname, mode_t flag,
                                      ERRNO* err_code);
TsFileWriter tsfile_writer_open_conf(const char* pathname, mode_t flag,
                                      ERRNO* err_code, TsFileConf* conf);

ERRNO tsfile_writer_close(TsFileWriter writer);
ERRNO tsfile_reader_close(TsFileReader reader);
