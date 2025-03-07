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

#include "tsfile_writer.h"

#include <unistd.h>

#include "chunk_writer.h"
#include "common/config/config.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "utils/errno_define.h"

using namespace common;

namespace storage {

int libtsfile_init() {
    static bool g_s_is_inited = false;
    if (g_s_is_inited) {
        return E_OK;
    }
    ModStat::get_instance().init();

    init_config_value();

    g_s_is_inited = true;
    return E_OK;
}

void libtsfile_destroy() { ModStat::get_instance().destroy(); }

void set_page_max_point_count(uint32_t page_max_ponint_count) {
    config_set_page_max_point_count(page_max_ponint_count);
}
void set_max_degree_of_index_node(uint32_t max_degree_of_index_node) {
    config_set_max_degree_of_index_node(max_degree_of_index_node);
}

TsFileWriter::TsFileWriter()
    : write_file_(nullptr),
      io_writer_(nullptr),
      schemas_(),
      start_file_done_(false),
      record_count_since_last_flush_(0),
      record_count_for_next_mem_check_(
          g_config_value_.record_count_for_next_mem_check_),
      write_file_created_(false) {}

TsFileWriter::~TsFileWriter() { destroy(); }

void TsFileWriter::destroy() {
    if (write_file_created_ && write_file_ != nullptr) {
        delete write_file_;
        write_file_ = nullptr;
    }
    if (io_writer_) {
        delete io_writer_;
        io_writer_ = nullptr;
    }
    DeviceSchemasMapIter dev_iter;
    // cppcheck-suppress postfixOperator
    for (dev_iter = schemas_.begin(); dev_iter != schemas_.end(); dev_iter++) {
        MeasurementSchemaMap &ms_map =
            dev_iter->second->measurement_schema_map_;
        MeasurementSchemaMapIter ms_iter;
        for (ms_iter = ms_map.begin(); ms_iter != ms_map.end(); ms_iter++) {
            MeasurementSchema *ms = ms_iter->second;
            if (ms != nullptr) {
                if (ms->chunk_writer_ != nullptr) {
                    delete ms->chunk_writer_;
                    ms->chunk_writer_ = nullptr;
                }
                delete ms;
                ms_iter->second = nullptr;
            }
        }
        delete dev_iter->second;
        dev_iter->second = nullptr;
    }
    schemas_.clear();
    record_count_since_last_flush_ = 0;
}

int TsFileWriter::init(WriteFile *write_file) {
    if (write_file == nullptr) {
        return E_INVALID_ARG;
    } else if (!write_file->file_opened()) {
        return E_INVALID_ARG;
    }
    write_file_ = write_file;
    write_file_created_ = false;
    io_writer_ = new TsFileIOWriter();
    io_writer_->init(write_file_);
    return E_OK;
}

void TsFileWriter::set_generate_table_schema(bool generate_table_schema) {
    io_writer_->set_generate_table_schema(generate_table_schema);
}

int TsFileWriter::register_table(
    const std::shared_ptr<TableSchema> &table_schema) {
    if (!table_schema) return E_INVALID_ARG;
    if (io_writer_->get_schema()->table_schema_map_.find(
            table_schema->get_table_name()) !=
        io_writer_->get_schema()->table_schema_map_.end()) {
        return E_ALREADY_EXIST;
    }
    io_writer_->get_schema()->register_table_schema(table_schema);
    return E_OK;
}

bool check_file_exist(const std::string &file_path) {
    return access(file_path.c_str(), F_OK) == 0;
}

int TsFileWriter::open(const std::string &file_path, int flags, mode_t mode) {
    if (check_file_exist(file_path)) {
        return E_ALREADY_EXIST;
    }
    write_file_ = new WriteFile;
    write_file_created_ = true;
    io_writer_ = new TsFileIOWriter;
    int ret = E_OK;
    if (RET_FAIL(write_file_->create(file_path, flags, mode))) {
    } else {
        io_writer_->init(write_file_);
    }
    return ret;
}

int TsFileWriter::open(const std::string &file_path) {
    return open(file_path, O_RDWR | O_CREAT | O_TRUNC, 0666);
}

int TsFileWriter::register_aligned_timeseries(
    const std::string &device_id, const MeasurementSchema &measurement_schema) {
    MeasurementSchema *ms = new MeasurementSchema(
        measurement_schema.measurement_name_, measurement_schema.data_type_,
        measurement_schema.encoding_, measurement_schema.compression_type_);
    return register_timeseries(device_id, ms, true);
}

int TsFileWriter::register_aligned_timeseries(
    const std::string &device_id,
    const std::vector<MeasurementSchema *> &measurement_schemas) {
    int ret = E_OK;
    for (auto it : measurement_schemas) {
        ret = register_timeseries(device_id, it, true);
        if (ret != E_OK) {
            return ret;
        }
    }
    return ret;
}

int TsFileWriter::register_timeseries(
    const std::string &device_id, const MeasurementSchema &measurement_schema) {
    MeasurementSchema *ms = new MeasurementSchema(
        measurement_schema.measurement_name_, measurement_schema.data_type_,
        measurement_schema.encoding_, measurement_schema.compression_type_);
    return register_timeseries(device_id, ms, false);
}

int TsFileWriter::register_timeseries(const std::string &device_path,
                                      MeasurementSchema *measurement_schema,
                                      bool is_aligned) {
    std::shared_ptr<IDeviceID> device_id =
        std::make_shared<StringArrayDeviceID>(device_path);
    DeviceSchemasMapIter device_iter = schemas_.find(device_id);
    if (device_iter != schemas_.end()) {
        MeasurementSchemaMap &msm =
            device_iter->second->measurement_schema_map_;
        MeasurementSchemaMapInsertResult ins_res = msm.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        if (UNLIKELY(!ins_res.second)) {
            return E_ALREADY_EXIST;
        }
    } else {
        MeasurementSchemaGroup *ms_group = new MeasurementSchemaGroup;
        ms_group->is_aligned_ = is_aligned;
        ms_group->measurement_schema_map_.insert(std::make_pair(
            measurement_schema->measurement_name_, measurement_schema));
        schemas_.insert(std::make_pair(device_id, ms_group));
    }
    return E_OK;
}

int TsFileWriter::register_timeseries(
    const std::string &device_id,
    const std::vector<MeasurementSchema *> &measurement_schema_vec) {
    int ret = E_OK;
    auto it = measurement_schema_vec.begin();
    for (; it != measurement_schema_vec.end();
         it++) {  // cppcheck-suppress postfixOperator
        ret = register_timeseries(device_id, *it);
        if (ret != E_OK) {
            return ret;
        }
    }
    return ret;
}

struct MeasurementSchemaMapNamesGetter {
   public:
    explicit MeasurementSchemaMapNamesGetter(
        const MeasurementSchemaMap &measurement_schema_map)
        : measurement_schema_map_(
              const_cast<MeasurementSchemaMap &>(measurement_schema_map)) {
        measurement_name_idx_ = measurement_schema_map_.begin();
    }

    FORCE_INLINE uint32_t get_count() const {
        return measurement_schema_map_.size();
    }

    FORCE_INLINE const std::string &next() {
        ASSERT(measurement_name_idx_ != measurement_schema_map_.end());
        std::string &ret = measurement_name_idx_->second->measurement_name_;
        measurement_name_idx_++;
        return ret;
    }

   private:
    MeasurementSchemaMap &measurement_schema_map_;
    MeasurementSchemaMap::iterator measurement_name_idx_;
};

struct MeasurementNamesFromRecord {
   public:
    explicit MeasurementNamesFromRecord(const TsRecord &record)
        : record_(record), measurement_name_idx_(0) {}
    FORCE_INLINE uint32_t get_count() const { return record_.points_.size(); }

    FORCE_INLINE const std::string &next() {
        return this->at(measurement_name_idx_++);
    }

   private:
    const TsRecord &record_;
    size_t measurement_name_idx_;
    FORCE_INLINE const std::string &at(size_t idx) const {
        ASSERT(idx < record_.points_.size());
        return record_.points_[idx].measurement_name_;
    }
};

struct MeasurementNamesFromTablet {
    explicit MeasurementNamesFromTablet(const Tablet &tablet)
        : tablet_(tablet), measurement_name_idx_(0) {}
    FORCE_INLINE uint32_t get_count() const {
        return tablet_.schema_vec_->size();
    }
    FORCE_INLINE const std::string &next() {
        return this->at(measurement_name_idx_++);
    }

   private:
    const Tablet &tablet_;
    size_t measurement_name_idx_;
    FORCE_INLINE const std::string &at(size_t idx) const {
        ASSERT(idx < tablet_.schema_vec_->size());
        return tablet_.schema_vec_->at(idx).measurement_name_;
    }
};

int TsFileWriter::do_check_and_prepare_tablet(Tablet &tablet) {
    if (tablet.column_categories_.empty()) {
        auto &schema_map = io_writer_->get_schema()->table_schema_map_;
        auto table_schema_it = schema_map.find(tablet.get_table_name());
        auto table_schema = table_schema_it->second;
        uint32_t column_cnt = tablet.get_column_count();
        for (uint32_t i = 0; i < column_cnt; i++) {
            auto &col_name = tablet.get_column_name(i);
            int col_index = table_schema->find_column_index(col_name);
            if (col_index == -1) {
                return E_COLUMN_NOT_EXIST;
            }
            const common::ColumnCategory column_category =
                table_schema->get_column_categories()[col_index];
            tablet.column_categories_.emplace_back(column_category);
            if (column_category == ColumnCategory::TAG) {
                tablet.id_column_indexes_.push_back(i);
            }
        }
    }
    return common::E_OK;
}

template <typename MeasurementNamesGetter>
int TsFileWriter::do_check_schema(std::shared_ptr<IDeviceID> device_id,
                                  MeasurementNamesGetter &measurement_names,
                                  SimpleVector<ChunkWriter *> &chunk_writers) {
    int ret = E_OK;
    DeviceSchemasMapIter dev_it = schemas_.find(device_id);
    MeasurementSchemaGroup *device_schema = nullptr;
    if (UNLIKELY(dev_it == schemas_.end()) ||
        IS_NULL(device_schema = dev_it->second)) {
        return E_DEVICE_NOT_EXIST;
    }
    MeasurementSchemaMap &msm = device_schema->measurement_schema_map_;
    uint32_t measurement_count = measurement_names.get_count();
    // chunk_writers.reserve(measurement_count);
    for (uint32_t i = 0; i < measurement_count; i++) {
        auto ms_iter = msm.find(measurement_names.next());
        if (UNLIKELY(ms_iter == msm.end())) {
            chunk_writers.push_back(NULL);
        } else {
            // In Java we will check data_type. But in C++, no check here.
            // Because checks are performed at the chunk layer and page layer
            MeasurementSchema *ms = ms_iter->second;
            if (IS_NULL(ms->chunk_writer_)) {
                ms->chunk_writer_ = new ChunkWriter;
                ret = ms->chunk_writer_->init(ms->measurement_name_,
                                              ms->data_type_, ms->encoding_,
                                              ms->compression_type_);
                if (IS_SUCC(ret)) {
                    chunk_writers.push_back(ms->chunk_writer_);
                } else {
                    for (size_t chunk_writer_idx = 0;
                         chunk_writer_idx < chunk_writers.size();
                         chunk_writer_idx++) {
                        if (!chunk_writers[chunk_writer_idx]) {
                            delete chunk_writers[chunk_writer_idx];
                        }
                    }
                    ret = common::E_INVALID_ARG;
                    return ret;
                }
            } else {
                chunk_writers.push_back(ms->chunk_writer_);
            }
        }
    }
    return ret;
}

template <typename MeasurementNamesGetter>
int TsFileWriter::do_check_schema_aligned(
    std::shared_ptr<IDeviceID> device_id,
    MeasurementNamesGetter &measurement_names,
    storage::TimeChunkWriter *&time_chunk_writer,
    common::SimpleVector<storage::ValueChunkWriter *> &value_chunk_writers) {
    int ret = E_OK;
    auto dev_it = schemas_.find(device_id);
    MeasurementSchemaGroup *device_schema = NULL;
    if (UNLIKELY(dev_it == schemas_.end()) ||
        IS_NULL(device_schema = dev_it->second)) {
        return E_DEVICE_NOT_EXIST;
    }
    if (IS_NULL(device_schema->time_chunk_writer_)) {
        device_schema->time_chunk_writer_ = new TimeChunkWriter();
        device_schema->time_chunk_writer_->init(
            "", g_config_value_.time_encoding_type_,
            g_config_value_.time_compress_type_);
    }
    time_chunk_writer = device_schema->time_chunk_writer_;
    MeasurementSchemaMap &msm = device_schema->measurement_schema_map_;
    uint32_t measurement_count = measurement_names.get_count();
    for (uint32_t i = 0; i < measurement_count; i++) {
        auto ms_iter = msm.find(measurement_names.next());
        if (UNLIKELY(ms_iter == msm.end())) {
            value_chunk_writers.push_back(NULL);
        } else {
            // Here we may check data_type against ms_iter. But in Java
            // libtsfile, no check here.
            MeasurementSchema *ms = ms_iter->second;
            if (IS_NULL(ms->value_chunk_writer_)) {
                ms->value_chunk_writer_ = new ValueChunkWriter;
                ret = ms->value_chunk_writer_->init(
                    ms->measurement_name_, ms->data_type_, ms->encoding_,
                    ms->compression_type_);
                if (IS_SUCC(ret)) {
                    value_chunk_writers.push_back(ms->value_chunk_writer_);
                } else {
                    value_chunk_writers.push_back(NULL);
                    for (size_t chunk_writer_idx = 0;
                         chunk_writer_idx < value_chunk_writers.size();
                         chunk_writer_idx++) {
                        if (!value_chunk_writers[chunk_writer_idx]) {
                            delete value_chunk_writers[chunk_writer_idx];
                        }
                    }
                    ret = common::E_INVALID_ARG;
                    return ret;
                }
            } else {
                value_chunk_writers.push_back(ms->value_chunk_writer_);
            }
        }
    }
    return ret;
}

int TsFileWriter::do_check_schema_table(
    std::shared_ptr<IDeviceID> device_id, Tablet &tablet,
    storage::TimeChunkWriter *&time_chunk_writer,
    common::SimpleVector<storage::ValueChunkWriter *> &value_chunk_writers) {
    int ret = E_OK;

    auto dev_it = schemas_.find(device_id);
    MeasurementSchemaGroup *device_schema = NULL;

    auto &schema_map = io_writer_->get_schema()->table_schema_map_;
    auto table_schema_it = schema_map.find(tablet.get_table_name());
    if (UNLIKELY(table_schema_it == schema_map.end())) {
        return E_TABLE_NOT_EXIST;
    }
    auto table_schema = table_schema_it->second;

    if (UNLIKELY(dev_it == schemas_.end()) ||
        IS_NULL(device_schema = dev_it->second)) {
        device_schema = new MeasurementSchemaGroup;
        device_schema->is_aligned_ = true;
        device_schema->time_chunk_writer_ = new TimeChunkWriter();
        device_schema->time_chunk_writer_->init(
            "", g_config_value_.time_encoding_type_,
            g_config_value_.time_compress_type_);

        for (uint32_t i = 0; i < table_schema->get_measurement_schemas().size();
             ++i) {
            if (table_schema->get_column_categories().at(i) ==
                common::ColumnCategory::FIELD) {
                auto table_column_schema =
                    table_schema->get_measurement_schemas().at(i);
                auto device_column_schema = new MeasurementSchema(
                    table_column_schema->measurement_name_,
                    table_column_schema->data_type_,
                    table_column_schema->encoding_,
                    table_column_schema->compression_type_);
                if (!table_column_schema->props_.empty()) {
                    device_column_schema->props_ = table_column_schema->props_;
                }
                device_schema->measurement_schema_map_
                    [device_column_schema->measurement_name_] =
                    device_column_schema;
            }
        }
        schemas_[device_id] = device_schema;
    }

    uint32_t column_cnt = tablet.get_column_count();
    time_chunk_writer = device_schema->time_chunk_writer_;
    MeasurementSchemaMap &msm = device_schema->measurement_schema_map_;

    for (uint32_t i = 0; i < column_cnt; i++) {
        if (tablet.column_categories_.at(i) != common::ColumnCategory::FIELD) {
            continue;
        }
        auto ms_iter = msm.find(tablet.get_column_name(i));
        if (UNLIKELY(ms_iter == msm.end())) {
            value_chunk_writers.push_back(NULL);
        } else {
            // Here we may check data_type against ms_iter. But in Java
            // libtsfile, no check here.
            MeasurementSchema *ms = ms_iter->second;
            if (IS_NULL(ms->value_chunk_writer_)) {
                ms->value_chunk_writer_ = new ValueChunkWriter;
                ret = ms->value_chunk_writer_->init(
                    ms->measurement_name_, ms->data_type_, ms->encoding_,
                    ms->compression_type_);
                if (IS_SUCC(ret)) {
                    value_chunk_writers.push_back(ms->value_chunk_writer_);
                } else {
                    value_chunk_writers.push_back(NULL);
                    for (size_t chunk_writer_idx = 0;
                         chunk_writer_idx < value_chunk_writers.size();
                         chunk_writer_idx++) {
                        if (!value_chunk_writers[chunk_writer_idx]) {
                            delete value_chunk_writers[chunk_writer_idx];
                        }
                    }
                    ret = common::E_INVALID_ARG;
                    return ret;
                }
            } else {
                value_chunk_writers.push_back(ms->value_chunk_writer_);
            }
        }
    }
    return ret;
}

int64_t TsFileWriter::calculate_mem_size_for_all_group() {
    int64_t mem_total_size = 0;
    DeviceSchemasMapIter device_iter;
    for (device_iter = schemas_.begin(); device_iter != schemas_.end();
         device_iter++) {
        MeasurementSchemaGroup *chunk_group = device_iter->second;
        MeasurementSchemaMap &map = chunk_group->measurement_schema_map_;
        for (MeasurementSchemaMapIter ms_iter = map.begin();
             ms_iter != map.end(); ms_iter++) {
            MeasurementSchema *m_schema = ms_iter->second;
            if (!chunk_group->is_aligned_) {
                ChunkWriter *&chunk_writer = m_schema->chunk_writer_;
                if (chunk_writer != nullptr) {
                    mem_total_size +=
                        chunk_writer->estimate_max_series_mem_size();
                }
            } else {
                ValueChunkWriter *&chunk_writer = m_schema->value_chunk_writer_;
                if (chunk_writer != nullptr) {
                    mem_total_size +=
                        chunk_writer->estimate_max_series_mem_size();
                }
            }
        }
        if (chunk_group->is_aligned_) {
            TimeChunkWriter *&time_chunk_writer =
                chunk_group->time_chunk_writer_;
            if (time_chunk_writer != nullptr) {
                mem_total_size +=
                    time_chunk_writer->estimate_max_series_mem_size();
            }
        }
    }
    return mem_total_size;
}

/**
 * check occupied memory size, if it exceeds the chunkGroupSize threshold, flush
 * them to given OutputStream.
 */
int TsFileWriter::check_memory_size_and_may_flush_chunks() {
    int ret = E_OK;
    if (record_count_since_last_flush_ >= record_count_for_next_mem_check_) {
        int64_t mem_size = calculate_mem_size_for_all_group();
        record_count_for_next_mem_check_ =
            record_count_since_last_flush_ *
            common::g_config_value_.chunk_group_size_threshold_ / mem_size;
        if (mem_size > common::g_config_value_.chunk_group_size_threshold_) {
            ret = flush();
        }
    }
    return ret;
}

int TsFileWriter::write_record(const TsRecord &record) {
    int ret = E_OK;
    // std::vector<ChunkWriter*> chunk_writers;
    SimpleVector<ChunkWriter *> chunk_writers;
    MeasurementNamesFromRecord mnames_getter(record);
    if (RET_FAIL(do_check_schema(
            std::make_shared<StringArrayDeviceID>(record.device_id_),
            mnames_getter, chunk_writers))) {
        return ret;
    }

    ASSERT(chunk_writers.size() == record.points_.size());
    for (uint32_t c = 0; c < chunk_writers.size(); c++) {
        ChunkWriter *chunk_writer = chunk_writers[c];
        if (IS_NULL(chunk_writer)) {
            continue;
        }
        // ignore point writer failure
        write_point(chunk_writer, record.timestamp_, record.points_[c]);
    }

    record_count_since_last_flush_++;
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

int TsFileWriter::write_record_aligned(const TsRecord &record) {
    int ret = E_OK;
    SimpleVector<ValueChunkWriter *> value_chunk_writers;
    TimeChunkWriter *time_chunk_writer;
    MeasurementNamesFromRecord mnames_getter(record);
    if (RET_FAIL(do_check_schema_aligned(
            std::make_shared<StringArrayDeviceID>(record.device_id_),
            mnames_getter, time_chunk_writer, value_chunk_writers))) {
        return ret;
    }
    if (value_chunk_writers.size() != record.points_.size()) {
        return E_INVALID_ARG;
    }
    time_chunk_writer->write(record.timestamp_);
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter *value_chunk_writer = value_chunk_writers[c];
        if (IS_NULL(value_chunk_writer)) {
            continue;
        }
        write_point_aligned(value_chunk_writer, record.timestamp_,
                            record.points_[c]);
    }
    return ret;
}

int TsFileWriter::write_point(ChunkWriter *chunk_writer, int64_t timestamp,
                              const DataPoint &point) {
    switch (point.data_type_) {
        case common::BOOLEAN:
            return chunk_writer->write(timestamp, point.u_.bool_val_);
        case common::INT32:
            return chunk_writer->write(timestamp, point.u_.i32_val_);
        case common::INT64:
            return chunk_writer->write(timestamp, point.u_.i64_val_);
        case common::FLOAT:
            return chunk_writer->write(timestamp, point.u_.float_val_);
        case common::DOUBLE:
            return chunk_writer->write(timestamp, point.u_.double_val_);
        case common::STRING:
            return chunk_writer->write(timestamp, *point.u_.str_val_);
        case common::TEXT:
            ASSERT(false);
            return E_OK;
        default:
            return E_INVALID_DATA_POINT;
    }
}

int TsFileWriter::write_point_aligned(ValueChunkWriter *value_chunk_writer,
                                      int64_t timestamp,
                                      const DataPoint &point) {
    bool isnull = point.isnull;
    switch (point.data_type_) {
        case common::BOOLEAN:
            return value_chunk_writer->write(timestamp, point.u_.bool_val_,
                                             isnull);
        case common::INT32:
            return value_chunk_writer->write(timestamp, point.u_.i32_val_,
                                             isnull);
        case common::INT64:
            return value_chunk_writer->write(timestamp, point.u_.i64_val_,
                                             isnull);
        case common::FLOAT:
            return value_chunk_writer->write(timestamp, point.u_.float_val_,
                                             isnull);
        case common::DOUBLE:
            return value_chunk_writer->write(timestamp, point.u_.double_val_,
                                             isnull);
        case common::TEXT:
            ASSERT(false);
            return E_OK;
        default:
            return E_INVALID_DATA_POINT;
    }
}

int TsFileWriter::write_tablet_aligned(const Tablet &tablet) {
    int ret = E_OK;
    SimpleVector<ValueChunkWriter *> value_chunk_writers;
    TimeChunkWriter *time_chunk_writer = nullptr;
    MeasurementNamesFromTablet mnames_getter(tablet);
    if (RET_FAIL(do_check_schema_aligned(
            std::make_shared<StringArrayDeviceID>(tablet.insert_target_name_),
            mnames_getter, time_chunk_writer, value_chunk_writers))) {
        return ret;
    }
    time_write_column(time_chunk_writer, tablet);
    ASSERT(value_chunk_writers.size() == tablet.get_column_count());
    for (uint32_t c = 0; c < value_chunk_writers.size(); c++) {
        ValueChunkWriter *value_chunk_writer = value_chunk_writers[c];
        if (IS_NULL(value_chunk_writer)) {
            continue;
        }
        value_write_column(value_chunk_writer, tablet, c);
    }
    return ret;
}

int TsFileWriter::write_tablet(const Tablet &tablet) {
    int ret = E_OK;
    SimpleVector<ChunkWriter *> chunk_writers;
    MeasurementNamesFromTablet mnames_getter(tablet);
    if (RET_FAIL(do_check_schema(
            std::make_shared<StringArrayDeviceID>(tablet.insert_target_name_),
            mnames_getter, chunk_writers))) {
        return ret;
    }
    ASSERT(chunk_writers.size() == tablet.get_column_count());
    for (uint32_t c = 0; c < chunk_writers.size(); c++) {
        ChunkWriter *chunk_writer = chunk_writers[c];
        if (IS_NULL(chunk_writer)) {
            continue;
        }
        // ignore writer failure
        write_column(chunk_writer, tablet, c);
    }

    record_count_since_last_flush_ += tablet.max_row_num_;
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

int TsFileWriter::write_table(Tablet &tablet) {
    int ret = E_OK;
    if (io_writer_->get_schema()->table_schema_map_.find(
            tablet.insert_target_name_) ==
        io_writer_->get_schema()->table_schema_map_.end()) {
        ret = E_TABLE_NOT_EXIST;
        return ret;
    }
    if (RET_FAIL(do_check_and_prepare_tablet(tablet))) {
        return ret;
    }

    auto device_id_end_index_pairs = split_tablet_by_device(tablet);
    int start_idx = 0;
    for (auto &device_id_end_index_pair : device_id_end_index_pairs) {
        auto device_id = device_id_end_index_pair.first;
        if (device_id_end_index_pair.second == 0) continue;
        if (table_aligned_) {
            SimpleVector<ValueChunkWriter *> value_chunk_writers;
            TimeChunkWriter *time_chunk_writer = nullptr;
            if (RET_FAIL(do_check_schema_table(device_id, tablet,
                                               time_chunk_writer,
                                               value_chunk_writers))) {
                return ret;
            }
            for (uint32_t i = 0; i < tablet.get_cur_row_size(); i++) {
                time_chunk_writer->write(tablet.timestamps_[i]);
            }
            uint32_t field_col_count = 0;
            for (uint32_t i = 0; i < tablet.get_column_count(); ++i) {
                if (tablet.column_categories_[i] ==
                    common::ColumnCategory::FIELD) {
                    ValueChunkWriter *value_chunk_writer =
                        value_chunk_writers[field_col_count];
                    if (IS_NULL(value_chunk_writer)) {
                        continue;
                    }
                    value_write_column(value_chunk_writer, tablet, i);
                    field_col_count++;
                }
            }
        } else {
            MeasurementNamesFromTablet mnames_getter(tablet);
            SimpleVector<ChunkWriter *> chunk_writers;
            if (RET_FAIL(
                    do_check_schema(device_id, mnames_getter, chunk_writers))) {
                return ret;
            }
            ASSERT(chunk_writers.size() == tablet.get_column_count());
            for (uint32_t c = 0; c < chunk_writers.size(); c++) {
                ChunkWriter *chunk_writer = chunk_writers[c];
                if (IS_NULL(chunk_writer)) {
                    continue;
                }
                write_column(chunk_writer, tablet, c, start_idx,
                             device_id_end_index_pair.second);
            }
            start_idx = device_id_end_index_pair.second;
        }
    }
    record_count_since_last_flush_ += tablet.cur_row_size_;
    ret = check_memory_size_and_may_flush_chunks();
    return ret;
}

std::vector<std::pair<std::shared_ptr<IDeviceID>, int>>
TsFileWriter::split_tablet_by_device(const Tablet &tablet) {
    std::vector<std::pair<std::shared_ptr<IDeviceID>, int>> result;
    std::shared_ptr<IDeviceID> last_device_id =
        std::make_shared<StringArrayDeviceID>("last_device_id");
    for (uint32_t i = 0; i < tablet.get_cur_row_size(); i++) {
        std::shared_ptr<IDeviceID> cur_device_id(tablet.get_device_id(i));
        if (*cur_device_id != *last_device_id) {
            result.emplace_back(std::move(last_device_id), i);
            last_device_id = std::move(cur_device_id);
        }
    }
    result.emplace_back(std::move(last_device_id), tablet.get_cur_row_size());
    return result;
}

int TsFileWriter::write_column(ChunkWriter *chunk_writer, const Tablet &tablet,
                               int col_idx, uint32_t start_idx,
                               uint32_t end_idx) {
    int ret = E_OK;

    common::TSDataType data_type = tablet.schema_vec_->at(col_idx).data_type_;
    int64_t *timestamps = tablet.timestamps_;
    Tablet::ValueMatrixEntry col_values = tablet.value_matrix_[col_idx];
    BitMap &col_notnull_bitmap = tablet.bitmaps_[col_idx];
    end_idx = std::min(end_idx, tablet.max_row_num_);

    if (data_type == common::BOOLEAN) {
        ret = write_typed_column(chunk_writer, timestamps, col_values.bool_data,
                                 col_notnull_bitmap, start_idx, end_idx);
    } else if (data_type == common::INT32) {
        ret =
            write_typed_column(chunk_writer, timestamps, col_values.int32_data,
                               col_notnull_bitmap, start_idx, end_idx);
    } else if (data_type == common::INT64) {
        ret =
            write_typed_column(chunk_writer, timestamps, col_values.int64_data,
                               col_notnull_bitmap, start_idx, end_idx);
    } else if (data_type == common::FLOAT) {
        ret =
            write_typed_column(chunk_writer, timestamps, col_values.float_data,
                               col_notnull_bitmap, start_idx, end_idx);
    } else if (data_type == common::DOUBLE) {
        ret =
            write_typed_column(chunk_writer, timestamps, col_values.double_data,
                               col_notnull_bitmap, start_idx, end_idx);
    } else if (data_type == common::STRING) {
        ret =
            write_typed_column(chunk_writer, timestamps, col_values.string_data,
                               col_notnull_bitmap, start_idx, end_idx);
    } else {
        ASSERT(false);
    }
    return ret;
}

int TsFileWriter::time_write_column(TimeChunkWriter *time_chunk_writer,
                                    const Tablet &tablet, uint32_t start_idx,
                                    uint32_t end_idx) {
    int64_t *timestamps = tablet.timestamps_;
    int ret = E_OK;
    if (IS_NULL(time_chunk_writer) || IS_NULL(timestamps)) {
        return E_INVALID_ARG;
    }
    for (uint32_t r = start_idx; r < end_idx && r < tablet.max_row_num_; r++) {
        if (RET_FAIL(time_chunk_writer->write(timestamps[r]))) {
            break;
        }
    }
    return ret;
}

int TsFileWriter::value_write_column(ValueChunkWriter *value_chunk_writer,
                                     const Tablet &tablet, int col_idx,
                                     uint32_t start_idx, uint32_t end_idx) {
    int ret = E_OK;

    TSDataType data_type = tablet.schema_vec_->at(col_idx).data_type_;
    int64_t *timestamps = tablet.timestamps_;
    Tablet::ValueMatrixEntry col_values = tablet.value_matrix_[col_idx];
    BitMap &col_notnull_bitmap = tablet.bitmaps_[col_idx];
    uint32_t row_count = tablet.max_row_num_;

    if (data_type == common::BOOLEAN) {
        ret = write_typed_column(value_chunk_writer, timestamps,
                                 (bool *)col_values.bool_data,
                                 col_notnull_bitmap, row_count);
    } else if (data_type == common::INT32) {
        ret = write_typed_column(value_chunk_writer, timestamps,
                                 (int32_t *)col_values.int32_data,
                                 col_notnull_bitmap, row_count);
    } else if (data_type == common::INT64) {
        ret = write_typed_column(value_chunk_writer, timestamps,
                                 (int64_t *)col_values.int64_data,
                                 col_notnull_bitmap, row_count);
    } else if (data_type == common::FLOAT) {
        ret = write_typed_column(value_chunk_writer, timestamps,
                                 (float *)col_values.float_data,
                                 col_notnull_bitmap, row_count);
    } else if (data_type == common::DOUBLE) {
        ret = write_typed_column(value_chunk_writer, timestamps,
                                 (double *)col_values.double_data,
                                 col_notnull_bitmap, row_count);
    } else {
        return E_NOT_SUPPORT;
    }
    return ret;
}

#define DO_WRITE_TYPED_COLUMN()                                          \
    do {                                                                 \
        int ret = E_OK;                                                  \
        for (uint32_t r = start_idx; r < end_idx; r++) {                 \
            if (LIKELY(!col_notnull_bitmap.test(r))) {                   \
                ret = chunk_writer->write(timestamps[r], col_values[r]); \
            }                                                            \
        }                                                                \
        return ret;                                                      \
    } while (false)

#define DO_VALUE_WRITE_TYPED_COLUMN()                                         \
    do {                                                                      \
        int ret = E_OK;                                                       \
        for (uint32_t r = 0; r < row_count; r++) {                            \
            if (LIKELY(col_notnull_bitmap.test(r))) {                         \
                ret = value_chunk_writer->write(timestamps[r], col_values[r], \
                                                true);                        \
            } else {                                                          \
                ret = value_chunk_writer->write(timestamps[r], col_values[r], \
                                                false);                       \
            }                                                                 \
        }                                                                     \
        return ret;                                                           \
    } while (false)

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, bool *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, int32_t *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, int64_t *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, float *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps, double *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ChunkWriter *chunk_writer,
                                     int64_t *timestamps,
                                     common::String *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t start_idx, uint32_t end_idx) {
    DO_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ValueChunkWriter *value_chunk_writer,
                                     int64_t *timestamps, bool *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t row_count) {
    DO_VALUE_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ValueChunkWriter *value_chunk_writer,
                                     int64_t *timestamps, int32_t *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t row_count) {
    DO_VALUE_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ValueChunkWriter *value_chunk_writer,
                                     int64_t *timestamps, int64_t *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t row_count) {
    DO_VALUE_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ValueChunkWriter *value_chunk_writer,
                                     int64_t *timestamps, float *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t row_count) {
    DO_VALUE_WRITE_TYPED_COLUMN();
}

int TsFileWriter::write_typed_column(ValueChunkWriter *value_chunk_writer,
                                     int64_t *timestamps, double *col_values,
                                     BitMap &col_notnull_bitmap,
                                     uint32_t row_count) {
    DO_VALUE_WRITE_TYPED_COLUMN();
}

// TODO make sure ret is meaningful to SDK user
int TsFileWriter::flush() {
    int ret = E_OK;
    if (!start_file_done_) {
        if (RET_FAIL(io_writer_->start_file())) {
            return ret;
        }
        start_file_done_ = true;
    }

    /* since @schemas_ used std::map which is rbtree underlying,
             so map itself is ordered by device name. */
    DeviceSchemasMapIter device_iter;
    for (device_iter = schemas_.begin(); device_iter != schemas_.end();
         device_iter++) {  // cppcheck-suppress postfixOperator
        if (device_iter->second->is_aligned_) {
            SimpleVector<ValueChunkWriter *> value_chunk_writers;
            TimeChunkWriter *time_chunk_writer;
            MeasurementSchemaMapNamesGetter mnames_getter(
                device_iter->second->measurement_schema_map_);
            if (RET_FAIL(do_check_schema_aligned(
                    device_iter->first, mnames_getter, time_chunk_writer,
                    value_chunk_writers))) {
                return ret;
            }
        } else {
            SimpleVector<ChunkWriter *> chunk_writers;
            MeasurementSchemaMapNamesGetter mnames_getter(
                device_iter->second->measurement_schema_map_);
            if (RET_FAIL(do_check_schema(device_iter->first, mnames_getter,
                                         chunk_writers))) {
                return ret;
            }
        }
        if (check_chunk_group_empty(device_iter->second,
                                    device_iter->second->is_aligned_)) {
            continue;
        }
        bool is_aligned = device_iter->second->is_aligned_;

        if (RET_FAIL(io_writer_->start_flush_chunk_group(device_iter->first,
                                                         is_aligned))) {
        } else if (RET_FAIL(
                       flush_chunk_group(device_iter->second, is_aligned))) {
        } else if (RET_FAIL(io_writer_->end_flush_chunk_group(is_aligned))) {
        }
    }
    record_count_since_last_flush_ = 0;
    return ret;
}

bool TsFileWriter::check_chunk_group_empty(MeasurementSchemaGroup *chunk_group,
                                           bool is_aligned) {
    MeasurementSchemaMap &map = chunk_group->measurement_schema_map_;
    for (MeasurementSchemaMapIter ms_iter = map.begin(); ms_iter != map.end();
         ms_iter++) {
        MeasurementSchema *m_schema = ms_iter->second;
        if (is_aligned) {
            if (m_schema->value_chunk_writer_ != NULL &&
                m_schema->value_chunk_writer_->hasData()) {
                return false;
            }
        } else {
            if (m_schema->chunk_writer_ != NULL &&
                m_schema->chunk_writer_->hasData()) {
                // first condition is to avoid first flush empty chunk group
                // second condition is to avoid repeated flush
                return false;
            }
        }
    }
    return true;
}

#define FLUSH_CHUNK(writer, io_writer, name, data_type, encoding, compression, \
                    num_pages)                                                 \
    if (RET_FAIL(writer->end_encode_chunk())) {                                \
    } else if (RET_FAIL(io_writer->start_flush_chunk(                          \
                   writer->get_chunk_data(), name, data_type, encoding,        \
                   compression, num_pages))) {                                 \
    } else if (RET_FAIL(io_writer->flush_chunk(writer->get_chunk_data()))) {   \
    } else if (RET_FAIL(io_writer->end_flush_chunk(                            \
                   writer->get_chunk_statistic()))) {                          \
    } else {                                                                   \
        writer->destroy();                                                     \
        delete writer;                                                         \
        writer = nullptr;                                                      \
    }

int TsFileWriter::flush_chunk_group(MeasurementSchemaGroup *chunk_group,
                                    bool is_aligned) {
    int ret = E_OK;
    MeasurementSchemaMap &map = chunk_group->measurement_schema_map_;

    if (chunk_group->is_aligned_) {
        TimeChunkWriter *&time_chunk_writer = chunk_group->time_chunk_writer_;
        ChunkHeader chunk_header = time_chunk_writer->get_chunk_header();
        FLUSH_CHUNK(time_chunk_writer, io_writer_,
                    chunk_header.measurement_name_, chunk_header.data_type_,
                    chunk_header.encoding_type_, chunk_header.compression_type_,
                    time_chunk_writer->num_of_pages())
    }

    for (MeasurementSchemaMapIter ms_iter = map.begin(); ms_iter != map.end();
         ms_iter++) {
        MeasurementSchema *m_schema = ms_iter->second;
        if (!chunk_group->is_aligned_) {
            ChunkWriter *&chunk_writer = m_schema->chunk_writer_;
            FLUSH_CHUNK(chunk_writer, io_writer_, m_schema->measurement_name_,
                        m_schema->data_type_, m_schema->encoding_,
                        m_schema->compression_type_,
                        chunk_writer->num_of_pages())
        } else {
            ValueChunkWriter *&value_chunk_writer =
                m_schema->value_chunk_writer_;
            FLUSH_CHUNK(value_chunk_writer, io_writer_,
                        m_schema->measurement_name_, m_schema->data_type_,
                        m_schema->encoding_, m_schema->compression_type_,
                        value_chunk_writer->num_of_pages())
        }
    }

    return ret;
}

int TsFileWriter::close() { return io_writer_->end_file(); }

}  // end namespace storage
