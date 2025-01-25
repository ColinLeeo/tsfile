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
#ifndef READER_TABLE_RESULT_H
#define READER_TABLE_RESULT_H
#include <memory>

#include "reader/block/tsblock_reader.h"
#include "reader/result_set.h"

namespace storage {
class TableResult : public ResultSet {
   public:
    explicit TableResult(std::unique_ptr<TsBlockReader> tsblock_reader,
                         std::vector<std::string> column_names,
                         std::vector<common::TSDataType> data_types) {}
    ~TableResult();
    bool next() override;
    bool is_null(const std::string& column_name) override;
    bool is_null(uint32_t column_index) override;
    RowRecord* get_row_record() override;
    ResultSetMetadata* get_metadata() override;
    void close() override;

   private:
    std::unique_ptr<TsBlockReader> tsblock_reader_;
    common::RowIterator* row_iterator_;
    std::vector<std::unique_ptr<TsBlockReader>> tsblock_readers_;
    std::vector<std::string> column_names_;
    std::vector<common::TSDataType> data_types_;
};
}  // namespace storage
#endif