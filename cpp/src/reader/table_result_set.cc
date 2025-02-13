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
#include "reader/table_result_set.h"

namespace storage {
void TableResultSet::init() {
    row_record_ = new RowRecord(column_names_.size());
    pa_.reset();
    pa_.init(512, common::MOD_TSFILE_READER);
    index_lookup_.reserve(column_names_.size());
    for (uint32_t i = 0; i < column_names_.size(); ++i) {
        index_lookup_.insert({column_names_[i], i});
    }
}

TableResultSet::~TableResultSet() {
    close();
}

bool TableResultSet::next() {
    while((row_iterator_ == nullptr || !row_iterator_->has_next()) && tsblock_reader_->has_next()) {
        if (tsblock_) {
            delete tsblock_;
            tsblock_ = nullptr;
        }
        if (tsblock_reader_->next(tsblock_) != common::E_OK) {
            break;
        }
        if (row_iterator_) {
            delete row_iterator_;
            row_iterator_ = nullptr;
        }
        row_iterator_ = new common::RowIterator(tsblock_);
    }
    if (row_iterator_ == nullptr || !row_iterator_->has_next()) {
        return false;
    }
    row_iterator_->next();
    uint32_t len = 0;
    for (uint32_t i = 0; i < row_iterator_->get_column_count(); ++i) {
        row_record_->get_field(i)->set_value(data_types_[i], row_iterator_->read(i, &len, nullptr), pa_);
    }
    return true;
}

bool TableResultSet::is_null(const std::string& column_name) {
    auto iter = index_lookup_.find(column_name);
    if (iter == index_lookup_.end()) {
        return true;
    } else {
        return is_null(iter->second);
    }
}

bool TableResultSet::is_null(uint32_t column_index) {
    return row_record_->get_field(column_index) == nullptr;
}

RowRecord* TableResultSet::get_row_record() {
    return row_record_;
}

ResultSetMetadata* TableResultSet::get_metadata() {
    return nullptr;
}

void TableResultSet::close() {
    tsblock_reader_->close();
    pa_.destroy();
    if (row_record_) {
        delete row_record_;
        row_record_ = nullptr;
    }
    if (row_iterator_) {
        delete row_iterator_;
        row_iterator_ = nullptr;
    }
    if (tsblock_) {
        delete tsblock_;
        tsblock_ = nullptr;
    } 
}



}