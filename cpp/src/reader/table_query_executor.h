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
#ifndef READER_TABLE_QUERY_EXECUTOR_H
#define READER_TABLE_QUERY_EXECUTOR_H

#include "common/schema.h"
#include "expression.h"
#include "imeta_data_querier.h"
#include "reader/block/device_ordered_tsblock_reader.h"
#include "reader/block/tsblock_reader.h"
#include "reader/column_mapping.h"
#include "reader/task/device_task_iterator.h"
#include "utils/errno_define.h"
namespace storage {

class DeviceTaskIterator;

class TableQueryExecutor {
   public:
    enum class TableQueryOrdering { TIME, DEVICE };

    TableQueryExecutor(std::shared_ptr<IMetadataQuerier> meta_data_querier,
                       std::shared_ptr<IChunkReader> chunk_reader_,
                       TableQueryOrdering table_query_ordering,
                       int block_size = 1024)
        : meta_data_querier_(std::move(meta_data_querier)),
          chunk_reader_(std::move(chunk_reader_)),
          table_query_ordering_(table_query_ordering),
          block_size_(block_size) {}

    int query(const std::string &table_name,
              const std::vector<std::string> &columns,
              const Filter *time_filter, const Filter *id_filter,
              const Filter *field_filter,
              std::unique_ptr<TsBlockReader> &ret_reader) {
        int ret = common::E_OK;
        TsFileMeta *file_metadata = nullptr;
        if (RET_FAIL(
                meta_data_querier_->get_whole_file_metadata(file_metadata))) {
            return ret;
        }
        common::PageArena pa;  // TODO: Optimize the memory allocation, use pa
                               // only to alloc String is not good
        pa.init(512, common::MOD_TSFILE_READER);
        common::String table_name_str;
        table_name_str.dup_from(table_name, pa);
        MetaIndexNode *table_root = nullptr;
        std::shared_ptr<TableSchema> table_schema;
        if (RET_FAIL(file_metadata->get_table_metaindex_node(table_name_str,
                                                             table_root))) {
        } else if (RET_FAIL(file_metadata->get_table_schema(table_name,
                                                            table_schema))) {
        }

        if (IS_FAIL(ret)) {
            ret_reader = std::unique_ptr<EmptyTsBlockReader>();
            return ret;
        }

        ColumnMapping column_mapping;
        for (size_t i = 0; i < columns.size(); ++i) {
            column_mapping.add(columns[i], static_cast<int>(i), *table_schema);
        }
        // column_mapping.add(*measurement_filter);

        auto device_task_iterator =
            std::unique_ptr<DeviceTaskIterator>(new DeviceTaskIterator(
                columns, table_root, column_mapping, meta_data_querier_,
                id_filter, *table_schema));

        switch (table_query_ordering_) {
            case TableQueryOrdering::DEVICE:
                ret_reader = std::unique_ptr<DeviceOrderedTsBlockReader>(
                    new DeviceOrderedTsBlockReader(
                        std::move(device_task_iterator), meta_data_querier_,
                        chunk_reader_, block_size_, time_filter, field_filter));
            case TableQueryOrdering::TIME:
            default:
                ret = common::E_UNSUPPORTED_ORDER;
        }

        return ret;
    }

   private:
    std::shared_ptr<IMetadataQuerier> meta_data_querier_;
    std::shared_ptr<IChunkReader> chunk_reader_;
    TableQueryOrdering table_query_ordering_;
    int32_t block_size_;
};

}  // namespace storage

#endif  // READER_TABLE_QUERY_EXECUTOR_H