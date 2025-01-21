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
#ifndef READER_SINGLE_DEVICE_TSBLOCK_READER_H
#define READER_SINGLE_DEVICE_TSBLOCK_READER_H

#include "common/tsblock/tsblock.h"
#include "reader/block/tsblock_reader.h"
#include "reader/filter/filter.h"
#include "reader/imeta_data_querier.h"
#include "reader/task/device_query_task.h"

namespace storage {

class DeviceQueryTask;
class IMetadataReader;
class MeasurementColumnContext;
class IdColumnContext;

class SingleDeviceTsBlockReader : public TsBlockReader {
   public:
    explicit SingleDeviceTsBlockReader(DeviceQueryTask* device_query_task,
                                       int32_t block_size,
                                       IMetadataReader* metadata_reader,
                                       IChunkReader* chunk_reader,
                                       Filter* time_filter,
                                       Filter* field_filter);
    bool has_next() override;
    int next(common::TsBlock& ret_block) override;
    void close() override;

   private:
    DeviceQueryTask* device_query_task_;
    Filter* field_filter_;
    int32_t block_size_;
    std::unique_ptr<common::TsBlock> current_block_;
    common::TupleDesc tuple_desc_;
    bool last_block_returned_ = true;
    std::map<std::string, MeasurementColumnContext*> field_column_contexts_;
    std::map<std::string, IdColumnContext*> id_column_contexts_;
    int64_t next_time_ = 0;
};

class MeasurementColumnContext {
   public:
    explicit MeasurementColumnContext(IChunkReader* chunk_reader)
        : chunk_reader_(chunk_reader) {}

    virtual ~MeasurementColumnContext() = default;

    virtual int remove_from(
        std::map<std::string, MeasurementColumnContext*>& column_contexts) = 0;
    virtual int fill_into(common::TsBlock& block, int32_t position) = 0;

   protected:
    IChunkReader* const chunk_reader_;
};

class SingleMeasurementColumnContext final : public MeasurementColumnContext {
   public:
    explicit SingleMeasurementColumnContext(IChunkReader* chunk_reader)
        : MeasurementColumnContext(chunk_reader) {}

    int remove_from(std::map<std::string, MeasurementColumnContext*>&
                        column_contexts) override;
    int fill_into(common::TsBlock& block, int32_t position) override;

   private:
    std::string column_name_;
    std::vector<int32_t> pos_in_result_;
};

class VectorMeasurementColumnContext final : public MeasurementColumnContext {
   public:
    explicit VectorMeasurementColumnContext(IChunkReader* chunk_reader)
        : MeasurementColumnContext(chunk_reader) {}

    int remove_from(std::map<std::string, MeasurementColumnContext*>&
                        column_contexts) override;
    int fill_into(common::TsBlock& block, int32_t position) override;

   private:
    std::vector<std::vector<int32_t>> pos_in_result_;
};

class IdColumnContext {
   public:
    explicit IdColumnContext(const std::vector<int32_t>& pos_in_result,
                             int32_t pos_in_device_id)
        : pos_in_result_(pos_in_result), pos_in_device_id_(pos_in_device_id) {}

   private:
    const std::vector<int32_t> pos_in_result_;
    const int32_t pos_in_device_id_;
};
}  // namespace storage

#endif  // READER_SINGLE_DEVICE_TSBLOCK_READER_H
