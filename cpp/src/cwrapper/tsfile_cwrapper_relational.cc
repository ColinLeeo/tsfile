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

#include "tsfile_cwrapper_relational.h"

#include <common/schema.h>
#include <writer/tsfile_writer.h>

void tsfile_writer_register_table(TsFileWriter writer, TableSchema *schema) {
    std::vector<storage::MeasurementSchema> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    for (int i = 0; i < schema->column_num; i++) {
        ColumnSchema* cur_schema = schema->column_schemas + i;
        measurement_schemas.emplace_back(cur_schema->column_name, static_cast<common::TSDataType>(cur_schema->data_type));
        column_categories.push_back(cur_schema->column_category);
    }
    auto tsfile_writer = static_cast<storage::TsFileWriter*>(writer);
    tsfile_writer->register_table(new TableSchema(schema->table_name, measurement_schemas));
}