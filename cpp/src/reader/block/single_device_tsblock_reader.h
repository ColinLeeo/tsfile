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
class MeasurementColumnContext;
class IdColumnContext;

class SingleDeviceTsBlockReader : public TsBlockReader {
   public:
    explicit SingleDeviceTsBlockReader(DeviceQueryTask* device_query_task,
                                       uint32_t block_size,
                                       IMetadataQuerier* metadata_querier,
                                       Filter* time_filter,
                                       Filter* field_filter);
    ~SingleDeviceTsBlockReader() { close(); }
    bool has_next() override;
    int next(common::TsBlock& ret_block) override;
    void close() override;

   private:
    void construct_column_context(
        const std::vector<std::shared_ptr<ChunkMeta>>& chunk_meta_list,
        Filter* time_filter);
    int fill_measurements(
        std::vector<MeasurementColumnContext*>& column_contexts);
    void fill_ids();
    void advance_column(MeasurementColumnContext* column_context);

    DeviceQueryTask* device_query_task_;
    Filter* field_filter_;
    uint32_t block_size_;
    common::TsBlock* current_block_;
    std::vector<common::ColAppender*> col_appenders_;
    common::RowAppender* row_appender_;
    common::TupleDesc tuple_desc_;
    bool last_block_returned_ = true;
    std::map<std::string, MeasurementColumnContext*> field_column_contexts_;
    std::map<std::string, IdColumnContext> id_column_contexts_;
    int64_t next_time_ = 0;
    TsFileIOReader* tsfile_io_reader_; // TODO: Construct Pass Ioreader
    common::PageArena pa_;
};

class MeasurementColumnContext {
   public:
    explicit MeasurementColumnContext(TsFileIOReader* tsfile_io_reader)
        : tsfile_io_reader_(tsfile_io_reader) {}

    virtual ~MeasurementColumnContext() = default;

    virtual void fill_into(
        std::vector<common::ColAppender*>& col_appenders) = 0;

    virtual void remove_from(std::map<std::string, MeasurementColumnContext*>&
                                 column_context_map) = 0;

    virtual int init(DeviceQueryTask* device_query_task,
                     const std::shared_ptr<ChunkMeta>& chunk_meta,
                     Filter* time_filter, common::PageArena& pa) = 0;
    virtual int get_next_tsblock(bool alloc_mem) = 0;

    virtual int get_current_time(int64_t& time) = 0;

    virtual int get_current_value(char* value) = 0;

    virtual int move_iter() = 0;

   protected:
    TsFileIOReader* tsfile_io_reader_;
    TsFileSeriesScanIterator* ssi_;
    common::TsBlock* tsblock_;
    common::ColIterator* time_iter_;
    common::ColIterator* value_iter_;
};

class SingleMeasurementColumnContext final : public MeasurementColumnContext {
   public:
    explicit SingleMeasurementColumnContext(TsFileIOReader* tsfile_io_reader)
        : MeasurementColumnContext(tsfile_io_reader) {}

    void fill_into(std::vector<common::ColAppender*>& col_appenders) override;
    void remove_from(std::map<std::string, MeasurementColumnContext*>&
                         column_context_map) override;
    int init(DeviceQueryTask* device_query_task,
             const std::shared_ptr<ChunkMeta>& chunk_meta, Filter* time_filter,
             common::PageArena& pa) override;
    int get_next_tsblock(bool alloc_mem) override;
    int get_current_time(int64_t& time) override;
    int get_current_value(char* value) override;
    int move_iter() override;

   private:
    std::string column_name_;
    std::vector<int32_t> pos_in_result_;
};

class VectorMeasurementColumnContext final : public MeasurementColumnContext {
   public:
    explicit VectorMeasurementColumnContext(TsFileIOReader* tsfile_io_reader)
        : MeasurementColumnContext(tsfile_io_reader) {}

    void fill_into(std::vector<common::ColAppender*>& col_appenders) override;
    void remove_from(std::map<std::string, MeasurementColumnContext*>&
                         column_context_map) override;
    int init(DeviceQueryTask* device_query_task,
             const std::shared_ptr<ChunkMeta>& chunk_meta, Filter* time_filter,
             common::PageArena& pa) override;
    int get_next_tsblock(bool alloc_mem) override;
    int get_current_time(int64_t& time) override;
    int get_current_value(char* value) override;
    int move_iter() override;

   private:
    std::vector<std::vector<int32_t>> pos_in_result_;
};

class IdColumnContext {
   public:
    explicit IdColumnContext(const std::vector<int32_t>& pos_in_result,
                             int32_t pos_in_device_id)
        : pos_in_result_(pos_in_result), pos_in_device_id_(pos_in_device_id) {}
    const std::vector<int32_t> pos_in_result_;
    const int32_t pos_in_device_id_;
};
}  // namespace storage

#endif  // READER_SINGLE_DEVICE_TSBLOCK_READER_H
