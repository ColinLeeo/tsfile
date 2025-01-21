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

#include "single_device_tsblock_reader.h"

namespace storage {

SingleDeviceTsBlockReader::SingleDeviceTsBlockReader(
    DeviceQueryTask* device_query_task,
    int32_t block_size, IMetadataReader* metadata_reader,
    IChunkReader* chunk_reader, Filter* time_filter, Filter* field_filter)
    : device_query_task_(device_query_task),
      field_filter_(field_filter),
      block_size_(block_size),
      tuple_desc_() {
    tuple_desc_.reset();
    tuple_desc_.push_back(common::g_time_column_desc);
    // for (const auto& column_name : device_query_task_->get_column_names()) {
        // common::ColumnDesc column_desc(common::INT64, common::TS_2DIFF, SNAPPY, INVALID_TTL, column_name, device_query_task_->get_device_id());
        // tuple_desc_.push_back(column_desc);
    // }
    current_block_ = std::unique_ptr<common::TsBlock>(
        common::TsBlock::create_tsblock(&tuple_desc_, block_size));
}

bool SingleDeviceTsBlockReader::has_next() {
    if (!last_block_returned_) {
        return true;
    }

    if (field_column_contexts_.empty()) {
        return false;
    }

    current_block_.reset();
    next_time_ = 0;

    std::vector<MeasurementColumnContext*> min_time_columns;
    while (current_block_->get_row_count() < block_size_) {
    }
    return true;
}

int SingleDeviceTsBlockReader::next(common::TsBlock& ret_block) {
    return common::E_OK;
}

void SingleDeviceTsBlockReader::close() {}

}  // namespace storage
