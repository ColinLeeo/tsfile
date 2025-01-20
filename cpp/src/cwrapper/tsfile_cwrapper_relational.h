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

typedef void *Tablet;

typedef struct column_schema {
    char *column_name;
    TSDataType data_type;
    ColumnCategory column_category;
} ColumnSchema;

typedef struct table_schema {
    char *table_name;
    ColumnSchema *column_schemas;
    int column_num;
} TableSchema;

#ifdef __cplusplus
extern "C" {
#endif


void tsfile_writer_register_table(TsFileWriter writer, TableSchema *schema);
ERRNO tsfile_write(TsFileWriter writer, Tablet *tablet);

ERRNO tsfile_writer_close(TsFileWriter writer);

TsFileReader tsfile_reader_new(char *file_name);

ResultSet tsfile_query(TsFileReader reader, char *table_name, char **columns,
                       uint32_t column_num, timestamp start_time,
                       timestamp end_time);

bool tsfile_result_set_next(ResultSet resultSet);

#define tsfile_result_set_get_value_by_name_(type)                       \
    type tsfile_result_set_get_value_by_name_##type(ResultSet resultSet, \
                                                    char* column_name);

tsfile_result_set_get_value_by_name_(int32_t)

tsfile_result_set_get_value_by_name_(int64_t)

tsfile_result_set_get_value_by_name_(float)

tsfile_result_set_get_value_by_name_(double)

tsfile_result_set_get_value_by_name_(bool)

#define tsfile_result_set_get_value_by_index_(type)                       \
    type tsfile_result_set_get_value_by_index_##type(ResultSet resultSet, \
                                                     uint32_t column_index);

tsfile_result_set_get_value_by_index_(int32_t)

tsfile_result_set_get_value_by_index_(int64_t)

tsfile_result_set_get_value_by_index_(float)

tsfile_result_set_get_value_by_index_(double)

tsfile_result_set_get_value_by_index_(bool)

bool tsfile_result_set_is_null_by_name(
    ResultSet resultSet,
    char *column_name);

bool tsfile_result_set_is_null_by_index(ResultSet resultSet,
                                        uint32_t column_index);

TableSchema *tsfile_reader_get_table_schema(TsFileReader reader,
                                            char *table_name);

void tsfile_result_set_close(ResultSet resultSet);

ResultSetMetaData tsfile_result_set_get_metadata(ResultSet resultSet);

char *tsfile_result_set_metadata_get_column_name(ResultSetMetaData metaData,
                                                 uint32_t index);

TSDataType tsfile_result_set_metadata_get_data_type(ResultSetMetaData metaData,
                                                    uint32_t index);

uint32_t tsfile_result_set_metadata_get_column_num(ResultSetMetaData metaData);

TableSchema *tsfile_reader_get_all_table_schema(TsFileReader reader,
                                                int32_t &schema_num);

void tsfile_reader_close(TsFileReader reader);

#ifdef __cplusplus
}
#endif
