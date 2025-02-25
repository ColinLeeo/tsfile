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
#include <string>
#include <vector>

#include "../c_examples/c_examples.h"
#include "cpp_examples.h"

std::string field_to_string(storage::Field* value) {
    if (value->type_ == common::TEXT) {
        return std::string(value->value_.sval_);
    } else {
        std::stringstream ss;
        switch (value->type_) {
            case common::BOOLEAN:
                ss << (value->value_.bval_ ? "true" : "false");
                break;
            case common::INT32:
                ss << value->value_.ival_;
                break;
            case common::INT64:
                ss << value->value_.lval_;
                break;
            case common::FLOAT:
                ss << value->value_.fval_;
                break;
            case common::DOUBLE:
                ss << value->value_.dval_;
                break;
            case common::NULL_TYPE:
                ss << "NULL";
                break;
            default:
                ASSERT(false);
                break;
        }
        return ss.str();
    }
}

int demo_read() {
    ERRNO code = 0;
    std::string table_name = "table1";
    storage::TsFileReader reader;
    reader.open("test.tsfile");
    storage::ResultSet* ret = nullptr;
    std::vector<std::string> columns;
    columns.push_back("id1");
    columns.push_back("id2");
    columns.push_back("s1");
    code = reader.query(table_name, columns, 0, 100, ret);
    auto metadata = ret->get_metadata();
    int column_num = metadata->get_column_count();
    for (int i = 0; i < column_num; i++) {
        std::cout << "column name: " << metadata->get_column_name(i)
                  << std::endl;
        std::cout << "column type: " << metadata->get_column_type(i)
                  << std::endl;
    }
    bool has_next = false;
    while (ret->next(has_next) && has_next) {
        Timestamp timestamp = ret->get_value<Timestamp>(1);
        for (int i = 0; i < column_num; i++) {
            if (ret->is_null(i)) {
                std::cout << "null" << std::endl;
            } else {
                switch (metadata->get_column_type(i)) {
                    case common::INT32:
                        std::cout << ret->get_value<int32_t>(i) << std::endl;
                        break;
                    case common::INT64:
                        std::cout << ret->get_value<int64_t>(i) << std::endl;
                        break;
                    case common::FLOAT:
                        std::cout << ret->get_value<float>(i) << std::endl;
                        break;
                    case common::DOUBLE:
                        std::cout << ret->get_value<double>(i) << std::endl;
                        break;
                    case common::STRING:
                        std::cout << ret->get_value<std::string>(i)
                                  << std::endl;
                        break;
                    default:;
                }
            }
        }
    }
    ret->close();
    reader.close();
}
