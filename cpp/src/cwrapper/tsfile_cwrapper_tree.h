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

#ifndef CWRAPPER_TSFILE_CWRAPPER_H
#define CWRAPPER_TSFILE_CWRAPPER_H

#include <fcntl.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#ifdef _WIN32
#include <sys/stat.h>
#endif

#include "tsfile_cwrapper_common.h"

typedef void* TsFileRowData;
typedef void* TimeFilterExpression;

#define MAX_COLUMN_FILTER_NUM 10

typedef struct timeseries_schema {
    char* name;
    TSDataType data_type;
    TSEncoding encoding;
    CompressionType compression;
} TimeseriesSchema;

typedef struct device_schema {
    char* device_name;
    TimeseriesSchema** timeseries_schema;
    int timeseries_num;
} DeviceSchema;

typedef enum operator_type {
    LT,
    LE,
    EQ,
    GT,
    GE,
    NOTEQ,
} OperatorType;

typedef enum expression_type {
    OR,
    AND,
    GLOBALTIME,
} ExpressionType;

typedef struct constant {
    int64_t value_condition;
    int type;
} Constant;

typedef struct expression {
    const char* column_name;
    Constant const_condition;
    ExpressionType expression_type;
    OperatorType operatype;
    struct expression* children[MAX_COLUMN_FILTER_NUM];
    int children_length;
} Expression;


typedef Tablet DataResult;

typedef void* QueryDataRetINTERNAL;
typedef struct query_data_ret {
    char** column_names;
    int column_num;
    QueryDataRetINTERNAL data;
}* QueryDataRet;

#ifdef __cplusplus
extern "C" {
#endif



ERRNO tsfile_register_timeseries(CTsFileWriter writer, const char* device_name,
                                 TimeseriesSchema* schema);
ERRNO tsfile_register_device(CTsFileWriter writer,
                                 DeviceSchema* device_schema);

TsFileRowData create_tsfile_row(const char* device_name, timestamp timestamp,
                                int timeseries_num);
#define insert_data_into_tsfile_row_by_name_(type)   \
    ERRNO insert_data_into_tsfile_row_by_name##type( \
        TsFileRowData data, char* sensor_name, type value);

insert_data_into_tsfile_row_by_name_(int32_t);
insert_data_into_tsfile_row_by_name_(int64_t);
insert_data_into_tsfile_row_by_name_(bool);
insert_data_into_tsfile_row_by_name_(float);
insert_data_into_tsfile_row_by_name_(double);

ERRNO tsfile_write_row_data(CTsFileWriter writer, TsFileRowData data);
ERRNO destroy_tsfile_row(TsFileRowData data);

Tablet* create_tablet(const char* table_name, int max_capacity);
Tablet* add_column_to_tablet(Tablet* tablet, const char* column_name,
                             TSDataType type);
Tablet add_data_to_tablet(Tablet tablet, int line_id, timestamp timestamp,
                          const char* column_name, int64_t value);

ERRNO destroy_tablet(Tablet* tablet);

ERRNO tsfile_flush_data(CTsFileWriter writer);

TimeFilterExpression* create_query_and_time_filter();

TimeFilterExpression* create_time_filter(const char* table_name,
                                         const char* column_name,
                                         OperatorType oper, timestamp timestamp);

TimeFilterExpression* add_time_filter_to_and_query(
    TimeFilterExpression* exp_and, TimeFilterExpression* exp);

void destroy_time_filter_query(TimeFilterExpression* expression);

Expression* create_time_expression(const char* column_name, OperatorType oper,
                                   timestamp timestamp);

Expression* add_and_filter_to_and_query(Expression* exp_and, Expression* exp);

QueryDataRet ts_reader_query(CTsFileReader reader, const char* table_name,
                             const char** columns, int colum_num,
                             TimeFilterExpression* expression);

QueryDataRet ts_reader_begin_end(CTsFileReader reader, const char* table_name,
                                 char** columns, int colum_num, timestamp begin,
                                 timestamp end);

QueryDataRet ts_reader_read(CTsFileReader reader, const char* table_name,
                            char** columns, int colum_num);

ERRNO destroy_query_data_ret(QueryDataRet query_data_set);

DataResult* ts_next(QueryDataRet data, int expect_line_count);

void print_data_result(DataResult* result);

void clean_data_record(DataResult data_result);
void clean_query_ret(QueryDataRet query_data_set);
void clean_query_tree(Expression* expression);

#ifdef __cplusplus
}
#endif
#endif  // CWRAPPER_TSFILE_CWRAPPER_H
