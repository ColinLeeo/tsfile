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

typedef void* Tablet;

typedef struct column_schema {
    char *column_name;
    TSDataType data_type;
    ColumnCategory column_category;
} ColumnSchema;

typedef struct table_schema {
    char* table_name;
    ColumnSchema* column_schemas;
    int column_num;
} TableSchema;


#ifdef __cplusplus
extern "C" {
#endif

ColumnSchema* column_schema_new(char* column_name, TSDataType column_type, ColumnCategory column_category);

char* column_schema_get_name(ColumnSchema* column_schema);

ColumnCategory column_schema_get_category(ColumnSchema* column_schema);

TableSchema* table_schema_new(char* table_name, ColumnSchema* column_schemas, int column_num);

CTsFileWriter tsfile_writer_new(char* file_name, TableSchema* schema, uint64_t threshold);

Tablet* tablet_new(char** column_name_list, TSDataType *data_type_list, int column_num, int max_capacity = 1024);

ERRNO tablet_add_timestamp(Tablet* tablet, uint32_t row_index, timestamp timestamp);

#define tablet_add_value_by_name_(type) \
    ERRNO tablet_add_value_by_name_##type(Tablet* tablet, uint32_t row_index, char* column_name, type value);

tablet_add_value_by_name_(int32_t)
tablet_add_value_by_name_(int64_t)
tablet_add_value_by_name_(float)
tablet_add_value_by_name_(double)
tablet_add_value_by_name_(bool)

#define table_add_value_by_index_(type) \
    ERRNO tablet_add_value_by_index_##type(Tablet* tablet, uint32_t row_index, uint32_t column_index, type value);

table_add_value_by_index_(int32_t)
table_add_value_by_index_(int64_t)
table_add_value_by_index_(float)
table_add_value_by_index_(double)
table_add_value_by_index_(bool)

ERRNO tsfile_write(CTsFileWriter writer, Tablet* tablet);
ERRNO tsfile_writer_close(CTsFileWriter writer);


CTsFileReader tsfile_reader_new(char* file_name);


ResultSet tsfile_query(CTsFileReader reader, char* table_name, char** columns, uint32_t column_num, timestamp start_time, timestamp end_time);

bool tsfile_result_set_next(ResultSet resultSet);

#define tsfile_result_set_get_value_by_name_(type) \
    type tsfile_result_set_get_value_by_name_##type(ResultSet resultSet, char* column_name);

tsfile_result_set_get_value_by_name_(int32_t)
tsfile_result_set_get_value_by_name_(int64_t)
tsfile_result_set_get_value_by_name_(float)
tsfile_result_set_get_value_by_name_(double)
tsfile_result_set_get_value_by_name_(bool)

#define tsfile_result_set_get_value_by_index_(type) \
    type tsfile_result_set_get_value_by_index_##type(ResultSet resultSet, uint32_t column_index);

tsfile_result_set_get_value_by_index_(int32_t)
tsfile_result_set_get_value_by_index_(int64_t)
tsfile_result_set_get_value_by_index_(float)
tsfile_result_set_get_value_by_index_(double)
tsfile_result_set_get_value_by_index_(bool)

bool tsfile_result_set_is_null_by_name(ResultSet resultSet, char* column_name);
bool tsfile_result_set_is_null_by_index(ResultSet resultSet, uint32_t column_index);

TableSchema* tsfile_reader_get_table_schema(CTsFileReader reader, char* table_name);
void tsfile_result_set_close(ResultSet resultSet);

ResultSetMetaData tsfile_result_set_get_metadata(ResultSet resultSet);

char* tsfile_result_set_metadata_get_column_name(ResultSetMetaData metaData, uint32_t index);
TSDataType tsfile_result_set_metadata_get_data_type(ResultSetMetaData metaData, uint32_t index);

uint32_t tsfile_result_set_metadata_get_column_num(ResultSetMetaData metaData);

TableSchema* tsfile_reader_get_all_table_schema(CTsFileReader reader, int32_t& schema_num);

void tsfile_reader_close(CTsFileReader reader);

#ifdef __cplusplus
}
#endif