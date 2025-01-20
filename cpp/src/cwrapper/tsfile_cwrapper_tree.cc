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

#include "tsfile_cwrapper_tree.h"

#include <iomanip>

#include "common/global.h"
#include "reader/expression.h"
#include "reader/filter/and_filter.h"
#include "reader/filter/filter.h"
#include "reader/filter/time_filter.h"
#include "reader/filter/time_operator.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"
#include "writer/tsfile_writer.h"

#define INSERT_DATA_INTO_RECORD(record, column, value)               \
    do {                                                             \
        DataPoint point(column, value);                              \
        if (record->points_.size() + 1 > record->points_.capacity()) \
            return E_BUF_NOT_ENOUGH;                                 \
        record->points_.push_back(point);                            \
        return E_OK;                                                 \
    } while (0)

#define CONSTRUCT_EXP_INTERNAL(exp, column_name) \
    do {                                         \
        exp.column_name = column_name;           \
        exp.operatype = oper;                    \
        exp.children_length = 0;                 \
    } while (0)

#define INSERT_DATA_TABLET_STEP                                             \
    do {                                                                    \
        for (int i = 0; i < tablet->column_num; i++) {                      \
            if (strcmp(tablet->timeseries_schema[i]->name, column_name) == 0) { \
                column_id = i;                                              \
                break;                                                      \
            }                                                               \
        }                                                                   \
        if (column_id == -1) {                                              \
            return tablet;                                                  \
        }                                                                   \
        if (tablet->cur_num + 1 > tablet->max_capacity) {                   \
            return tablet;                                                  \
        }                                                                   \
        tablet->times[line_id] = timestamp;                                 \
    } while (0)

#define DataType common::TSDataType
#define Encoding common::TSEncoding
#define CompressionType common::CompressionType
#define E_OK common::E_OK
#define TsRecord storage::TsRecord
#define DataPoint storage::DataPoint
#define E_BUF_NOT_ENOUGH common::E_BUF_NOT_ENOUGH

ERRNO tsfile_register_timeseries(TsFileWriter writer,
                                       const char* device_name,
                                       TimeseriesSchema* schema) {
    auto* w = (storage::TsFileWriter*)writer;

    int ret = w->register_timeseries(
        device_name, storage::MeasurementSchema(
                        schema->name, (DataType)schema->data_type,
                        (Encoding)schema->encoding, (CompressionType)schema->compression));
    return ret;
}

ERRNO tsfile_register_device(TsFileWriter writer,
                                device_schema* device_schema) {
    storage::TsFileWriter* w = (storage::TsFileWriter*)writer;
    for (int column_id = 0; column_id < device_schema->timeseries_num; column_id++) {
        TimeseriesSchema* schema = device_schema->timeseries_schema[column_id];
        ERRNO ret = w->register_timeseries(
            device_schema->device_name,
            storage::MeasurementSchema(
                schema->name, (DataType)schema->data_type,
                        (Encoding)schema->encoding, (CompressionType)schema->compression));
        if (ret != E_OK) {
            return ret;
        }
    }
    return E_OK;
}



TsFileRowData create_tsfile_row(const char* table_name, int64_t timestamp,
                                int timeseries_num) {
    TsRecord* record = new TsRecord(timestamp, table_name, timeseries_num);
    return record;
}

int get_size_from_datatype(TSDataType datatype) {
    switch(datatype) {
        case TSDataType::TS_DATATYPE_INT32:
            return sizeof(int32_t);
        case TSDataType::TS_DATATYPE_INT64:
            return sizeof(int64_t);
        case TSDataType::TS_DATATYPE_FLOAT:
            return sizeof(float);
        case TSDataType::TS_DATATYPE_DOUBLE:
            return sizeof(double);
        case TSDataType::TS_DATATYPE_BOOLEAN:
            return sizeof(bool);
        case TSDataType::TS_DATATYPE_TEXT:
            return sizeof(char*);
        default:
            return 0;
    }
    return 0;
}

ERRNO insert_data_into_tsfile_row_int32(TsFileRowData data, char* columname,
                                            int32_t value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ERRNO insert_data_into_tsfile_row_boolean(TsFileRowData data,
                                              char* columname, bool value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ERRNO insert_data_into_tsfile_row_int64(TsFileRowData data, char* columname,
                                            int64_t value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ERRNO insert_data_into_tsfile_row_float(TsFileRowData data, char* columname,
                                            float value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ERRNO insert_data_into_tsfile_row_double(TsFileRowData data,
                                             char* columname, double value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ERRNO tsfile_write_row_data(TsFileWriter writer, TsFileRowData data) {
    storage::TsFileWriter* w = (storage::TsFileWriter*)writer;
    TsRecord* record = (TsRecord*)data;
    int ret = w->write_record(*record);
    if (ret == E_OK) {
        delete record;
    }
    return ret;
}

ERRNO destory_tsfile_row(TsFileRowData data) {
    TsRecord* record = (TsRecord*)data;
    if (record != nullptr) {
        delete record;
        record = nullptr;
    }
    return E_OK;
}

ERRNO tsfile_flush_data(TsFileWriter writer) {
    storage::TsFileWriter* w = (storage::TsFileWriter*)writer;
    int ret = w->flush();
    return ret;
}

Expression create_column_filter(const char* column_name, OperatorType oper,
                                int32_t int32_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = int32_value;
    exp.const_condition.type = TSDataType::TS_DATATYPE_INT32;
    return exp;
}

Expression create_column_filter(const char* column_name, OperatorType oper,
                                int64_t int64_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = int64_value;
    exp.const_condition.type = TSDataType::TS_DATATYPE_INT64;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                bool bool_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = bool_value ? 1 : 0;
    exp.const_condition.type = TSDataType::TS_DATATYPE_BOOLEAN;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                float float_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    memcpy(&exp.const_condition.value_condition, &float_value, sizeof(float));
    exp.const_condition.type = TSDataType::TS_DATATYPE_FLOAT;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                double double_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = double_value;
    exp.const_condition.type = TSDataType::TS_DATATYPE_DOUBLE;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                const char* char_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = reinterpret_cast<int64_t>(char_value);
    exp.const_condition.type = TSDataType::TS_DATATYPE_TEXT;
    return exp;
}

TimeFilterExpression* create_andquery_timefilter() {
    storage::Expression* exp = new storage::Expression(storage::AND_EXPR);
    return (TimeFilterExpression*)exp;
}

TimeFilterExpression* create_time_filter(const char* table_name,
                                         const char* column_name,
                                         OperatorType oper, int64_t timestamp) {
    std::string table_name_str(table_name);
    std::string column_name_str(column_name);
    storage::Path path(table_name_str, column_name_str);
    storage::Filter* filter;
    switch (oper) {
        case GT:
            filter = storage::TimeFilter::gt(timestamp);
            break;
        case LT:
            filter = storage::TimeFilter::lt(timestamp);
            break;
        case EQ:
            filter = storage::TimeFilter::eq(timestamp);
            break;
        case NOTEQ:
            filter = storage::TimeFilter::not_eqt(timestamp);
            break;
        case GE:
            filter = storage::TimeFilter::gt_eq(timestamp);
            break;
        case LE:
            filter = storage::TimeFilter::lt_eq(timestamp);
            break;
        default:
            filter = nullptr;
            break;
    }
    storage::Expression* exp =
        new storage::Expression(storage::SERIES_EXPR, path, filter);
    return (TimeFilterExpression*)exp;
}

TimeFilterExpression* add_time_filter_to_and_query(
    TimeFilterExpression* exp_and, TimeFilterExpression* exp) {
    storage::Expression* and_exp = (storage::Expression*)exp_and;
    storage::Expression* time_exp = (storage::Expression*)exp;
    if (and_exp->left_ == nullptr) {
        and_exp->left_ = time_exp;
    } else if (and_exp->right_ == nullptr) {
        and_exp->right_ = time_exp;
    } else {
        storage::Expression* new_exp =
            new storage::Expression(storage::AND_EXPR);
        new_exp->left_ = and_exp->right_;
        and_exp->right_ = new_exp;
        add_time_filter_to_and_query((TimeFilterExpression*)new_exp, exp);
    }
    return exp_and;
}

void destory_time_filter_query(TimeFilterExpression* expression) {
    if (expression == nullptr) {
        return;
    }

    destory_time_filter_query(
        (TimeFilterExpression*)((storage::Expression*)expression)->left_);
    destory_time_filter_query(
        (TimeFilterExpression*)((storage::Expression*)expression)->right_);
    storage::Expression* exp = (storage::Expression*)expression;
    if (exp->type_ == storage::ExpressionType::SERIES_EXPR) {
        delete exp->filter_;
    } else {
        delete exp;
    }
}

Expression create_global_time_expression(OperatorType oper, int64_t timestamp) {
    Expression exp;
    exp.operatype = oper;
    exp.expression_type = GLOBALTIME;
    exp.const_condition.value_condition = timestamp;
    exp.const_condition.type = TSDataType::TS_DATATYPE_INT64;
    return exp;
}

Expression* and_filter_to_and_query(Expression* exp_and, Expression* exp) {
    if (exp_and->children_length >= MAX_COLUMN_FILTER_NUM - 1) {
        return nullptr;
    }
    exp_and->children[exp_and->children_length++] = exp;
    return exp_and;
}

QueryDataRet ts_reader_query(TsFileReader reader, const char* table_name,
                             const char** columns_name, int column_num,
                             TimeFilterExpression* expression) {
    TsFileReader* r = (TsFileReader*)reader;
    std::string table_name_str(table_name);
    std::vector<storage::Path> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns_name[i]);
        selected_paths.push_back(storage::Path(table_name_str, column_name));
    }

    storage::ResultSet* qds = nullptr;
    storage::QueryExpression* query_expression =
        storage::QueryExpression::create(selected_paths,
                                         (storage::Expression*)expression);
    r->query(query_expression, qds);
    QueryDataRet ret = (QueryDataRet)malloc(sizeof(struct query_data_ret));
    ret->data = qds;
    ret->column_names = (char**)malloc(column_num * sizeof(char*));
    ret->column_num = column_num;
    for (int i = 0; i < column_num; i++) {
        ret->column_names[i] = strdup(columns_name[i]);
    }
    return ret;
}

QueryDataRet ts_reader_begin_end(TsFileReader reader, const char* table_name,
                                 char** columns_name, int column_num,
                                 timestamp begin, timestamp end) {
    TsFileReader* r = (TsFileReader*)reader;
    std::string table_name_str(table_name);
    std::vector<storage::Path> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns_name[i]);
        selected_paths.push_back(storage::Path(table_name_str, column_name));
    }

    storage::ResultSet* qds = nullptr;
    storage::Filter* filter_low = nullptr;
    storage::Filter* filter_high = nullptr;
    storage::Expression* exp = nullptr;
    storage::Filter* and_filter = nullptr;
    if (begin != -1) {
        filter_low = storage::TimeFilter::gt_eq(begin);
    }
    if (end != -1) {
        filter_high = storage::TimeFilter::lt_eq(end);
    }
    if (filter_low != nullptr && filter_high != nullptr) {
        and_filter = new storage::AndFilter(filter_low, filter_high);
        exp = new storage::Expression(storage::GLOBALTIME_EXPR,
                                      and_filter);  // exp never be deleted
    } else if (filter_low != nullptr && filter_high == nullptr) {
        exp = new storage::Expression(storage::GLOBALTIME_EXPR, filter_low);
    } else if (filter_high != nullptr && filter_low == nullptr) {
        exp = new storage::Expression(storage::GLOBALTIME_EXPR, filter_high);
    }
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(selected_paths, exp);
    r->query(query_expr, qds);
    QueryDataRet ret = (QueryDataRet)malloc(sizeof(struct query_data_ret));
    ret->data = qds;
    ret->column_num = column_num;
    ret->column_names = (char**)malloc(column_num * sizeof(char*));
    for (int i = 0; i < column_num; i++) {
        ret->column_names[i] = strdup(columns_name[i]);
    }
    return ret;
}

QueryDataRet ts_reader_read(TsFileReader reader, const char* table_name,
                            char** columns_name, int column_num) {
    TsFileReader* r = (TsFileReader*)reader;
    std::string table_name_str(table_name);
    std::vector<storage::Path> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns_name[i]);
        selected_paths.push_back(storage::Path(table_name_str, column_name));
    }
    storage::ResultSet* qds = nullptr;
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(selected_paths, nullptr);
    r->query(query_expr, qds);
    QueryDataRet ret = (QueryDataRet)malloc(sizeof(struct query_data_ret));
    ret->data = qds;
    ret->column_names = (char**)malloc(column_num * sizeof(char*));
    ret->column_num = column_num;
    for (int i = 0; i < column_num; i++) {
        ret->column_names[i] = strdup(columns_name[i]);
    }
    return ret;
}

ERRNO destory_query_dataret(QueryDataRet data) {
    storage::ResultSet* qds = (storage::ResultSet*)data->data;
    delete qds;
    for (int i = 0; i < data->column_num; i++) {
        free(data->column_names[i]);
    }
    free(data->column_names);
    free(data);
    return E_OK;
}

DataResult* ts_next(QueryDataRet data, int expect_line_count) {
    storage::ResultSet* qds = (storage::ResultSet*)data->data;
    DataResult* result = create_tablet("result", expect_line_count);
    storage::RowRecord* record;
    bool init_tablet = false;
    for (int i = 0; i < expect_line_count; i++) {
        if (!qds->next()) {
            break;
        }
        record = qds->get_row_record();
        int column_num = record->get_fields()->size();
        if (!init_tablet) {
            for (int col = 0; col < column_num; col++) {
                storage::Field* field = record->get_field(col);
                result = add_column_to_tablet(result, data->column_names[col],
                                              (TSDataType)field->type_);
            }
            init_tablet = true;
        }
        for (int col = 0; col < column_num; col++) {
            storage::Field* field = record->get_field(col);
            switch (field->type_) {
                // all data will stored as 8 bytes
                case DataType::BOOLEAN:
                    result = add_data_to_tablet_bool(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.bval_);
                    break;
                case DataType::INT32:
                    result = add_data_to_tablet_i32(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.ival_);
                    break;
                case DataType::INT64:
                    result = add_data_to_tablet_i64(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.lval_);
                    break;
                case DataType::FLOAT:
                    result = add_data_to_tablet_float(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.fval_);
                    break;
                case DataType::DOUBLE:
                    result = add_data_to_tablet_double(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.dval_);
                    break;
                case DataType::TEXT:
                    result = add_data_to_tablet_char(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.sval_);
                    break;
                case DataType::NULL_TYPE:
                    // result = add_data_to_tablet(result, i ,
                    // record->get_timestamp(),
                    //                             data->column_names[col], 0);
                    // skip null data
                    break;
                default:
                    std::cout << field->type_ << std::endl;
                    std::cout << "error here" << std::endl;
                    return nullptr;
            }
        }
    }
    return result;
}

void print_data_result(DataResult* result) {
    std::cout << std::left << std::setw(15) << "timestamp";
    for (int i = 0; i < result->column_num; i++) {
        std::cout << std::left << std::setw(15)
                  << result->timeseries_schema[i]->name;
    }
    std::cout << std::endl;
    for (int i = 0; i < result->cur_num; i++) {
        std::cout << std::left << std::setw(15);
        std::cout << result->times[i];
        for (int j = 0; j < result->column_num; j++) {
            timeseries_schema* schema = result->timeseries_schema[j];
            double dval;
            float fval;
            std::cout << std::left << std::setw(15);
            switch (schema->data_type) {
                case TSDataType::TS_DATATYPE_BOOLEAN:
                    std::cout
                        << ((*((int64_t*)result->value[j] + i)) > 0 ? "true"
                                                                    : "false");
                    break;
                case TSDataType::TS_DATATYPE_INT32:
                    std::cout << *((int64_t*)result->value[j] + i);
                    break;
                case TSDataType::TS_DATATYPE_INT64:
                    std::cout << *((int64_t*)result->value[j] + i);
                    break;
                case TSDataType::TS_DATATYPE_FLOAT:
                    memcpy(&fval, (int64_t*)result->value[j] + i,
                           sizeof(float));
                    std::cout << fval;
                    break;
                case TSDataType::TS_DATATYPE_DOUBLE:
                    memcpy(&dval, (int64_t*)result->value[j] + i,
                           sizeof(double));
                    std::cout << dval;
                    break;
                default:
                    std::cout << "";
            }
        }
        std::cout << std::endl;
    }
}