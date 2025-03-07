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

#include "file/tsfile_io_reader.h"

#include "common/allocator/alloc_base.h"

using namespace common;

namespace storage {
int TsFileIOReader::init(const std::string &file_path) {
  int ret = E_OK;
  read_file_ = new ReadFile;
  read_file_created_ = true;
  if (RET_FAIL(read_file_->open(file_path))) {
  }
  return ret;
}

int TsFileIOReader::init(ReadFile *read_file) {
  if (IS_NULL(read_file)) {
    ASSERT(false);
    return E_INVALID_ARG;
  }
  read_file_created_ = false;
  read_file_ = read_file;
  return E_OK;
}

void TsFileIOReader::reset() {
  if (read_file_ != nullptr) {
    if (read_file_created_) {
      read_file_->destroy();
      delete read_file_;
    }
    read_file_ = nullptr;
    tsfile_meta_page_arena_.destroy();
    tsfile_meta_ready_ = false;
  }
}

int TsFileIOReader::alloc_ssi(std::shared_ptr<IDeviceID> device_id,
                              const std::string &measurement_name,
                              TsFileSeriesScanIterator *&ssi,
                              common::PageArena &pa, Filter *time_filter) {
  int ret = E_OK;
  if (RET_FAIL(load_tsfile_meta_if_necessary())) {
  } else {
    ssi = new TsFileSeriesScanIterator;
    ssi->init(device_id, measurement_name, read_file_, time_filter, pa);
    if (RET_FAIL(load_timeseries_index_for_ssi(device_id,
                                               measurement_name, ssi))) {
    } else if (time_filter != nullptr &&
        !filter_stasify(ssi->itimeseries_index_, time_filter)) {
      ret = E_NO_MORE_DATA;
    } else if (RET_FAIL(ssi->init_chunk_reader())) {
    }
    if (ret != E_OK) {
      ssi->destroy();
      delete ssi;
      ssi = nullptr;
    }
  }
  return ret;
}

void TsFileIOReader::revert_ssi(TsFileSeriesScanIterator *ssi) {
  if (ssi != nullptr) {
    ssi->destroy();
    delete ssi;
  }
}

int TsFileIOReader::get_device_timeseries_meta_without_chunk_meta(
    std::shared_ptr<IDeviceID> device_id,
    std::vector<ITimeseriesIndex *> &timeseries_indexs,
    PageArena &pa) {
  int ret = E_OK;
  load_tsfile_meta_if_necessary();
  DeviceMetaIndexEntry meta_index_entry;
  int64_t end_offset;
  std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t> >
      meta_index_entry_list;
  if (RET_FAIL(
      load_device_index_entry(std::make_shared<DeviceIDComparable>(device_id),
                              meta_index_entry, end_offset))) {
  } else if (RET_FAIL(load_all_measurement_index_entry(
      meta_index_entry.offset_, end_offset, pa,
      meta_index_entry_list))) {
  } else if (RET_FAIL(do_load_all_timeseries_index(meta_index_entry_list, pa,
                                                   timeseries_indexs))) {
  }
  return ret;
}

bool TsFileIOReader::filter_stasify(ITimeseriesIndex *ts_index,
                                    Filter *time_filter) {
  ASSERT(ts_index->get_statistic() != nullptr);
  return time_filter->satisfy(ts_index->get_statistic());
}

int TsFileIOReader::load_tsfile_meta_if_necessary() {
  int ret = E_OK;
  if (!tsfile_meta_ready_) {
    if (RET_FAIL(load_tsfile_meta())) {
      // log_err("load_tsfile_meta error, ret=%d", ret);
      return ret;
    } else {
      tsfile_meta_ready_ = true;
    }
  }
  return ret;
}

int TsFileIOReader::load_tsfile_meta() {
  const int32_t TSFILE_READ_IO_SIZE = 1024; // TODO make it configurable
  const int32_t TAIL_MAGIC_AND_META_SIZE_SIZE =
      10;                  // magic(6B) + meta_size(4B)
  ASSERT(file_size() > 0); // > 13

  int ret = E_OK;
  uint32_t tsfile_meta_size = 0;
  int32_t read_offset = 0;
  int32_t ret_read_len = 0;

  // Step 1: reader the tsfile_meta_size
  // 1.1 prepare reader buffer
  int32_t alloc_size = UTIL_MIN(TSFILE_READ_IO_SIZE, file_size());
  char *read_buf = (char *) mem_alloc(alloc_size, MOD_TSFILE_READER);
  if (IS_NULL(read_buf)) {
    return E_OOM;
  }
  // 1.2 reader data from file
  read_offset = file_size() - alloc_size;
  ret_read_len = 0;
  if (RET_FAIL(read_file_->read(read_offset, read_buf, alloc_size,
                                ret_read_len))) {
  } else if (ret_read_len != alloc_size) {
    ret = E_FILE_READ_ERR;
    // log_err("do not reader enough data from tsfile, want-size=%d,
    // reader-size=%d, file=%s", alloc_size, ret_read_len,
    // get_file_path().c_str());
  }
  // 1.3 deserialize tsfile_meta_size
  if (IS_SUCC(ret)) {
    // deserialize tsfile_meta_size
    char *size_buf = read_buf + alloc_size - TAIL_MAGIC_AND_META_SIZE_SIZE;
    tsfile_meta_size = SerializationUtil::read_ui32(size_buf);
    ASSERT(tsfile_meta_size > 0 && tsfile_meta_size <= (1ll << 20));
  }

  // Step 2: reader TsFileMeta
  if (IS_SUCC(ret)) {
    // 2.1 prepare enough buffer (use the previous buffer if can).
    char *tsfile_meta_buf = nullptr;
    if (tsfile_meta_size + TAIL_MAGIC_AND_META_SIZE_SIZE >
        (uint32_t) alloc_size) {
      // prepare buffer to re-reader from start of tsfile_meta
      char *old_read_buf = read_buf;
      read_buf = (char *) mem_realloc(read_buf, tsfile_meta_size);
      if (IS_NULL(read_buf)) {
        read_buf = old_read_buf;
        ret = E_OOM;
      } else if (RET_FAIL(read_file_->read(
          file_size() - tsfile_meta_size -
              TAIL_MAGIC_AND_META_SIZE_SIZE,
          read_buf, tsfile_meta_size, ret_read_len))) {
      } else if (tsfile_meta_size != (uint32_t) ret_read_len) {
        ret = E_FILE_READ_ERR;
        // log_err("do not reader enough data from tsfile, want-size=%d,
        // reader-size=%d, file=%s", tsfile_meta_size, ret_read_len,
        // get_file_path().c_str());
      } else {
        tsfile_meta_buf = read_buf;
      }
    } else {
      // the previous buffer has contained the TsFileMeta data
      tsfile_meta_buf = read_buf + alloc_size - tsfile_meta_size -
          TAIL_MAGIC_AND_META_SIZE_SIZE;
      // DEBUG_hex_dump_buf("tsfile_meta_buf=", tsfile_meta_buf,
      // tsfile_meta_size);
    }
    if (IS_SUCC(ret)) {
      ByteStream tsfile_meta_bs;
      tsfile_meta_bs.wrap_from(tsfile_meta_buf, tsfile_meta_size);
      if (RET_FAIL(tsfile_meta_.deserialize_from(tsfile_meta_bs))) {
      }
#if DEBUG_SE
      std::cout << "load tsfile_meta, ret=" << ret
                << ", tsfile_meta_=" << tsfile_meta_ << std::endl;
#endif
    }
  }
  mem_free(read_buf);
  return ret;
}

int TsFileIOReader::load_timeseries_index_for_ssi(
    std::shared_ptr<IDeviceID> device_id, const std::string &measurement_name,
    TsFileSeriesScanIterator *&ssi) {
  int ret = E_OK;
  DeviceMetaIndexEntry device_index_entry;
  int64_t device_ie_end_offset = 0;
  MeasurementMetaIndexEntry measurement_index_entry;
  int64_t measurement_ie_end_offset = 0;
  // bool is_aligned = false;
  if (RET_FAIL(load_device_index_entry(
      std::make_shared<DeviceIDComparable>(device_id), device_index_entry,
      device_ie_end_offset))) {
  } else if (RET_FAIL(load_measurement_index_entry(
      measurement_name, device_index_entry.offset_,
      device_ie_end_offset, measurement_index_entry,
      measurement_ie_end_offset))) {
  } else if (RET_FAIL(do_load_timeseries_index(
      measurement_name, measurement_index_entry.offset_,
      measurement_ie_end_offset, ssi->timeseries_index_pa_,
      ssi->itimeseries_index_))) {
  } else {
#if DEBUG_SE
    if (measurement_index_entry.name_.len_) {
        std::cout << "load timeseries index: "
                  << *((TimeseriesIndex *)ssi->itimeseries_index_)
                  << std::endl;
    } else {
        std::cout << "load aligned timeseries index: "
                  << *((AlignedTimeseriesIndex *)ssi->itimeseries_index_)
                  << std::endl;
    }
#endif
  }
  return ret;
}

int TsFileIOReader::load_device_index_entry(
    std::shared_ptr<IComparable> device_name,
    IMetaIndexEntry &device_index_entry, int64_t &end_offset) {
  int ret = E_OK;
  std::shared_ptr<DeviceIDComparable> device_id_comparable =
      std::dynamic_pointer_cast<DeviceIDComparable>(device_name);
  if (device_id_comparable == nullptr) {
    return E_INVALID_DATA_POINT;
  }
  auto index_node = tsfile_meta_.table_metadata_index_node_map_[device_id_comparable->device_id_->get_table_name()];
  assert(tsfile_meta_.table_metadata_index_node_map_.find(device_id_comparable->device_id_->get_table_name()) !=
        tsfile_meta_.table_metadata_index_node_map_.end());
  assert(index_node != nullptr);
  if (index_node->node_type_ == LEAF_DEVICE) {
    // FIXME
    ret = index_node->binary_search_children(
        device_name, true, device_index_entry, end_offset);
  } else {
    ret = search_from_internal_node(device_name, index_node,
                                    device_index_entry, end_offset);
  }
  if (ret == E_NOT_EXIST) {
    ret = E_DEVICE_NOT_EXIST;
  }
#if DEBUG_SE
  std::cout << "load_device_index_entry, device_index_entry={"
            << device_index_entry << "}, end_offset=" << end_offset
            << std::endl;
#endif
  return ret;
}

int TsFileIOReader::load_measurement_index_entry(
    const std::string &measurement_name_str, int64_t start_offset,
    int64_t end_offset, IMetaIndexEntry &ret_measurement_index_entry,
    int64_t &ret_end_offset) {
#if DEBUG_SE
  std::cout << "load_measurement_index_entry: measurement_name_str="
            << measurement_name_str << ", start_offset=" << start_offset
            << ", end_offset=" << end_offset << std::endl;
#endif
  ASSERT(start_offset < end_offset);
  int ret = E_OK;

  // 1. load top measuremnt_index_node
  PageArena pa;
  pa.init(512, MOD_TSFILE_READER);
  const int32_t read_size = (int32_t) (end_offset - start_offset);
  int32_t ret_read_len = 0;
  char *data_buf = (char *) pa.alloc(read_size);
  void *m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
  if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
    return E_OOM;
  }
  auto *top_node_ptr = new(m_idx_node_buf) MetaIndexNode(&pa);
  auto top_node = std::shared_ptr<MetaIndexNode>(
      top_node_ptr, MetaIndexNode::self_deleter);

  if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                ret_read_len))) {
  } else if (RET_FAIL(top_node->deserialize_from(data_buf, read_size))) {
  }
#if DEBUG_SE
  std::cout
      << "load_measurement_index_entry deserialize MetaIndexNode, top_node="
      << *top_node << " at file pos " << start_offset << " to " << end_offset
      << std::endl;
#endif
  // 2. search from top_node in top-down way
  if (IS_SUCC(ret)) {
    auto measurement_name =
        std::make_shared<StringComparable>(measurement_name_str);
    if (top_node->node_type_ == LEAF_MEASUREMENT) {
      ret = top_node->binary_search_children(
          measurement_name, /*exact*/ false, ret_measurement_index_entry,
          ret_end_offset);
    } else {
      ret = search_from_internal_node(measurement_name, top_node,
                                      ret_measurement_index_entry,
                                      ret_end_offset);
    }
  }
  if (ret == E_NOT_EXIST) {
    ret = E_MEASUREMENT_NOT_EXIST;
  }
  return ret;
}

int TsFileIOReader::load_all_measurement_index_entry(
    int64_t start_offset, int64_t end_offset, common::PageArena &pa,
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t> >
    &ret_measurement_index_entry) {
#if DEBUG_SE
  std::cout << "load_measurement_index_entry: measurement_name_str= "
            << ", start_offset=" << start_offset
            << ", end_offset=" << end_offset << std::endl;
#endif
  ASSERT(start_offset < end_offset);
  int ret = E_OK;
  // 1. load top measuremnt_index_node
  const int32_t read_size = (int32_t) (end_offset - start_offset);
  int32_t ret_read_len = 0;
  char *data_buf = (char *) pa.alloc(read_size);
  void *m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
  if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
    return E_OOM;
  }
  auto *top_node_ptr = new(m_idx_node_buf) MetaIndexNode(&pa);
  auto top_node = std::shared_ptr<MetaIndexNode>(
      top_node_ptr, MetaIndexNode::self_deleter);
  if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                ret_read_len))) {
  } else if (RET_FAIL(top_node->deserialize_from(data_buf, read_size))) {
  }
#if DEBUG_SE
  std::cout
      << "load_measurement_index_entry deserialize MetaIndexNode, top_node="
      << *top_node << " at file pos " << start_offset << " to " << end_offset
      << std::endl;
#endif
  // 2. search from top_node in top-down way
  if (IS_SUCC(ret)) {
    get_all_leaf(top_node, ret_measurement_index_entry);
  }
  if (ret == E_NOT_EXIST) {
    ret = E_MEASUREMENT_NOT_EXIST;
  }
  return ret;
}

int TsFileIOReader::read_device_meta_index(int32_t start_offset,
                                           int32_t end_offset,
                                           common::PageArena &pa,
                                           MetaIndexNode *&device_meta_index) {
    int ret = E_OK;
    ASSERT(start_offset < end_offset);
    const int32_t read_size = (int32_t)(end_offset - start_offset);
    int32_t ret_read_len = 0;
    char *data_buf = (char *)pa.alloc(read_size);
    void *m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
    if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
        return E_OOM;
    }
    device_meta_index = new (m_idx_node_buf) MetaIndexNode(&pa);
    if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                  ret_read_len))) {
    } else if (RET_FAIL(
                   device_meta_index->deserialize_from(data_buf, read_size))) {
    }
    return ret;
}

int TsFileIOReader::get_timeseries_indexes(std::shared_ptr<IDeviceID> device_id,
                             const std::unordered_set<std::string> &measurement_names,
                             std::vector<ITimeseriesIndex *> &timeseries_indexs,
                             common::PageArena &pa) {
  int ret = E_OK;
  DeviceMetaIndexEntry device_index_entry;
  int64_t device_ie_end_offset = 0;
  MeasurementMetaIndexEntry measurement_index_entry;
  int64_t measurement_ie_end_offset = 0;
  if (RET_FAIL(load_device_index_entry(
      std::make_shared<DeviceIDComparable>(device_id), device_index_entry,
      device_ie_end_offset))) {
    return ret;
  }
  int64_t idx = 0;
  for (const auto &measurement_name : measurement_names) {
    if (RET_FAIL(load_measurement_index_entry(
        measurement_name, device_index_entry.offset_,
        device_ie_end_offset, measurement_index_entry,
        measurement_ie_end_offset))) {
    } else if (RET_FAIL(do_load_timeseries_index(
        measurement_name, measurement_index_entry.offset_,
        measurement_ie_end_offset, pa,
        timeseries_indexs[idx++]))) {
    }
  }
  return ret;
}

/*
 * @target_name device_name or measurement_name
 * @index_node  leaf device node or leaf measurement node
 */
int TsFileIOReader::search_from_leaf_node(
    std::shared_ptr<IComparable> target_name,
    std::shared_ptr<MetaIndexNode> index_node,
    IMetaIndexEntry &ret_index_entry, int64_t &ret_end_offset) {
  int ret = E_OK;
  ret = index_node->binary_search_children(target_name, true, ret_index_entry,
                                           ret_end_offset);
  return ret;
}

int TsFileIOReader::search_from_internal_node(
    std::shared_ptr<IComparable> target_name,
    std::shared_ptr<MetaIndexNode> index_node, IMetaIndexEntry &ret_index_entry,
    int64_t &ret_end_offset) {
  int ret = E_OK;
  IMetaIndexEntry index_entry;
  int64_t end_offset = 0;

  ASSERT(index_node->node_type_ == INTERNAL_MEASUREMENT ||
      index_node->node_type_ == INTERNAL_DEVICE);

  if (RET_FAIL(index_node->binary_search_children(
      target_name, /*exact=*/false, index_entry, end_offset))) {
    return ret;
  }

  while (IS_SUCC(ret)) {
    // reader next level index node
    const int read_size = end_offset - index_entry.get_offset();
#if DEBUG_SE
    std::cout << "search_from_internal_node, end_offset=" << end_offset
              << ", index_entry.offset_=" << index_entry.get_offset()
              << std::endl;
#endif
    ASSERT(read_size > 0 && read_size < (1 << 30));
    PageArena cur_level_index_node_pa;
    void *buf = cur_level_index_node_pa.alloc(sizeof(MetaIndexNode));
    char *data_buf = (char *) cur_level_index_node_pa.alloc(read_size);
    if (IS_NULL(buf) || IS_NULL(data_buf)) {
      return E_OOM;
    }
    MetaIndexNode *cur_level_index_node =
        new(buf) MetaIndexNode(&cur_level_index_node_pa);
    int32_t ret_read_len = 0;
    if (RET_FAIL(read_file_->read(index_entry.get_offset(), data_buf, read_size,
                                  ret_read_len))) {
    } else if (read_size != ret_read_len) {
      ret = E_TSFILE_CORRUPTED;
    } else if (RET_FAIL(cur_level_index_node->deserialize_from(
        data_buf, read_size))) {
    } else {
      if (cur_level_index_node->node_type_ == LEAF_DEVICE) {
        ret = cur_level_index_node->binary_search_children(
            target_name, /*exact=*/true, ret_index_entry,
            ret_end_offset);
        cur_level_index_node->destroy();
        return ret; //// FIXME
      } else if (cur_level_index_node->node_type_ == LEAF_MEASUREMENT) {
        ret = cur_level_index_node->binary_search_children(
            target_name, /*exact=*/false, ret_index_entry,
            ret_end_offset);
        cur_level_index_node->destroy();
        return ret; //// FIXME
      } else {
        ret = cur_level_index_node->binary_search_children(
            target_name, /*exact=*/false, index_entry, end_offset);
        cur_level_index_node->destroy();
      }
    }
  }
  return ret;
}

int TsFileIOReader::do_load_timeseries_index(
    const std::string &measurement_name_str, int64_t start_offset,
    int64_t end_offset, PageArena &in_timeseries_index_pa,
    ITimeseriesIndex *&ret_timeseries_index) {
  ASSERT(end_offset > start_offset);
  int ret = E_OK;
  int32_t read_size = (int32_t) (end_offset - start_offset);
  int32_t ret_read_len = 0;
  char *ti_buf = (char *) mem_alloc(read_size, MOD_TSFILE_READER);
  if (IS_NULL(ti_buf)) {
    return E_OOM;
  }
  if (RET_FAIL(
      read_file_->read(start_offset, ti_buf, read_size, ret_read_len))) {
  } else {
    ByteStream bs;
    bs.wrap_from(ti_buf, read_size);
    const String target_measurement_name(
        (char *) measurement_name_str.c_str(),
        strlen(measurement_name_str.c_str()));
    bool found = false;
#if DEBUG_SE
    std::cout << "do_load_timeseries_index, reader file at " << start_offset
              << " to " << end_offset << std::endl;
#endif
    bool is_aligned = false;
    AlignedTimeseriesIndex *aligned_ts_idx = nullptr;
    while (IS_SUCC(ret)) {
      TimeseriesIndex cur_timeseries_index;
      PageArena cur_timeseries_index_pa;
      cur_timeseries_index_pa.init(512, MOD_TSFILE_READER); // TODO 512
      if (RET_FAIL(cur_timeseries_index.deserialize_from(
          bs, &cur_timeseries_index_pa))) {
      } else if (is_aligned ||
          cur_timeseries_index.get_data_type() == common::VECTOR) {
        if (!is_aligned) {
          is_aligned = true;
          void *buf = in_timeseries_index_pa.alloc(
              sizeof(AlignedTimeseriesIndex));
          aligned_ts_idx = new(buf) AlignedTimeseriesIndex;
          buf = in_timeseries_index_pa.alloc(sizeof(TimeseriesIndex));
          aligned_ts_idx->time_ts_idx_ = new(buf) TimeseriesIndex;
          aligned_ts_idx->time_ts_idx_->clone_from(
              cur_timeseries_index, &in_timeseries_index_pa);
        } else if (cur_timeseries_index.get_measurement_name().equal_to(
            target_measurement_name)) {
          void *buf =
              in_timeseries_index_pa.alloc(sizeof(TimeseriesIndex));
          aligned_ts_idx->value_ts_idx_ = new(buf) TimeseriesIndex;
          aligned_ts_idx->value_ts_idx_->clone_from(
              cur_timeseries_index, &in_timeseries_index_pa);
          ret_timeseries_index = aligned_ts_idx;
          found = true;
          break;
        }
      } else if (!is_aligned &&
          cur_timeseries_index.get_measurement_name().equal_to(
              target_measurement_name)) {
        void *buf =
            in_timeseries_index_pa.alloc(sizeof(TimeseriesIndex));
        auto ts_idx = new(buf) TimeseriesIndex;
        ts_idx->clone_from(cur_timeseries_index,
                           &in_timeseries_index_pa);
        ret_timeseries_index = ts_idx;
        found = true;
        break;
      }
    } // end while
    if (!found) {
      ret = E_NOT_EXIST;
    }
  }
  mem_free(ti_buf);
  return ret;
}

int TsFileIOReader::do_load_all_timeseries_index(
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t> > &
    index_node_entry_list,
    common::PageArena &in_timeseries_index_pa,
    std::vector<ITimeseriesIndex *> &ts_indexs) {
  int ret = E_OK;
  for (const auto &index_node_entry : index_node_entry_list) {
    int64_t start_offset = index_node_entry.first->get_offset(),
        end_offset = index_node_entry.second;
    const std::string target_measurement_name(
        index_node_entry.first->get_name().to_std_string());
    ITimeseriesIndex *ts_idx;
    ret = do_load_timeseries_index(target_measurement_name, start_offset,
                                   end_offset, in_timeseries_index_pa,
                                   ts_idx);
    if (IS_SUCC(ret)) {
      ts_indexs.push_back(ts_idx);
    }
  }
  return ret;
}

int TsFileIOReader::get_all_leaf(
    std::shared_ptr<MetaIndexNode> index_node,
    std::vector<std::pair<std::shared_ptr<IMetaIndexEntry>, int64_t> > &
    index_node_entry_list) {
  int ret = E_OK;
  if (index_node->node_type_ == LEAF_MEASUREMENT ||
      index_node->node_type_ == LEAF_DEVICE) {
    for (size_t i = 0; i < index_node->children_.size(); i++) {
      if (i + 1 < index_node->children_.size()) {
        index_node_entry_list.push_back(
            std::make_pair(index_node->children_[i],
                           index_node->children_[i + 1]->get_offset()));
      } else {
        index_node_entry_list.push_back(std::make_pair(
            index_node->children_[i], index_node->end_offset_));
      }
    }
  } else {
    // read next level index node
    for (size_t i = 0; i < index_node->children_.size(); i++) {
      int64_t end_offset = index_node->end_offset_;
      if (i + 1 < index_node->children_.size()) {
        end_offset = index_node->children_[i + 1]->get_offset();
      }
      const int read_size =
          end_offset - index_node->children_[i]->get_offset();
#if DEBUG_SE
      std::cout << "search_from_internal_node, end_offset=" << end_offset
                << ", index_entry.offset_=" << index_node->children_[i]->get_offset()
                << std::endl;
#endif
      ASSERT(read_size > 0 && read_size < (1 << 30));
      PageArena cur_level_index_node_pa;
      void *buf = cur_level_index_node_pa.alloc(sizeof(MetaIndexNode));
      char *data_buf = (char *) cur_level_index_node_pa.alloc(read_size);
      if (IS_NULL(buf) || IS_NULL(data_buf)) {
        return E_OOM;
      }
      auto *cur_level_index_node_ptr = new(buf) MetaIndexNode(
          &cur_level_index_node_pa);
      auto cur_level_index_node = std::shared_ptr<MetaIndexNode>(
          cur_level_index_node_ptr, MetaIndexNode::self_deleter);

      int32_t ret_read_len = 0;
      if (RET_FAIL(
          read_file_->read(index_node->children_[i]->get_offset(),
                           data_buf, read_size, ret_read_len))) {
      } else if (read_size != ret_read_len) {
        ret = E_TSFILE_CORRUPTED;
      } else if (RET_FAIL(cur_level_index_node->deserialize_from(
          data_buf, read_size))) {
      } else {
        ret = get_all_leaf(cur_level_index_node, index_node_entry_list);
      }
    }
  }
  return ret;
}
#if 0
int TsFileIOReader::get_next(const std::string &device_path,
                             const std::string &measurement_name,
                             TsBlock *ret_tsblock,
                             TimeRange &ret_time_range)
{
  int ret = E_OK;
  if (RET_FAIL(load_timeseries_index_if_necessary(device_path, measurement_name))) {
    return ret;
  }
  return get_next_page(ret_tsblock);
}

int TsFileIOReader::get_next_page(TsBlock *ret_tsblock)
{
  int ret = E_OK;

  if (!chunk_reader_.has_more_data()) {
    // has finished reading current chunk
    if (has_next_chunk()) {
      cursor_to_next_chunk();
      ChunkMeta *next_chunk_meta = chunk_meta_cursor_.get();
      if (RET_FAIL(init_next_chunk_reader(next_chunk_meta))) {
      }
    } else {
      // has finished reading all chunks of this tsfile
      ret = E_NO_MORE_DATA;
    }
  } // end if (!chunk_reader_.has_more_data())

  if (IS_SUCC(ret)) {
    ret = chunk_reader_.get_next_page(ret_tsblock);
  }
  return ret;
}

int TsFileIOReader::init_first_chunk_reader(ChunkMeta *cm,
                                            ReadFile *read_file,
                                            const ColumnDesc &col_desc)
{
  ASSERT(!chunk_reader_.has_more_data());
  int ret = E_OK;
  if (RET_FAIL(chunk_reader_.init(read_file,
                                  timeseries_index_.get_measurement_name(),
                                  col_desc.type_,
                                  col_desc.encoding_))) {
  } else if (RET_FAIL(chunk_reader_.load_by_meta(cm))) {
  }
  return ret;
}

int TsFileIOReader::init_next_chunk_reader(ChunkMeta *cm)
{
  ASSERT(!chunk_reader_.has_more_data());
  chunk_reader_.reset();
  return chunk_reader_.load_by_meta(cm);
}

int TsFileIOReader::load_timeseries_index_if_necessary(const std::string &device_path,
                                                       const std::string &measurement_name)
{
  int ret = E_OK;
  if (col_desc.ts_id_ != timeseries_index_.get_ts_id()) {
    if (RET_FAIL(load_timeseries_index(col_desc))) {
      //log_err("load timeseries_index error, ret=%d", ret);
    } else {
      chunk_meta_cursor_ = timeseries_index_.get_chunk_meta_list()->begin();
      ChunkMeta *next_chunk_meta = chunk_meta_cursor_.get();
      if (RET_FAIL(init_first_chunk_reader(next_chunk_meta, read_file_, col_desc))) {
      } else {
        cursor_to_next_chunk();
      }
    }
  } else {
    // timeseries_index_ is ready
  }
  return ret;
}

// TODO add a result cache for load_timeseries_index
int TsFileIOReader::load_timeseries_index(const ColumnDesc &col_desc)
{
  int ret = E_OK;

  if (RET_FAIL(load_tsfile_meta_if_necessary())) {
    return ret;
  }

  MetaIndexEntry device_index_entry;
  int64_t device_ie_end_offset = 0;
  MetaIndexEntry measurement_index_entry;
  int64_t measurement_ie_end_offset = 0;

  if (RET_FAIL(load_device_index_entry(col_desc, device_index_entry, device_ie_end_offset))) {
  } else if (RET_FAIL(load_measurement_index_entry(col_desc, device_index_entry.offset_,
                                                   device_ie_end_offset, measurement_index_entry,
                                                   measurement_ie_end_offset))) {
  } else if (RET_FAIL(do_load_timeseries_index(col_desc, measurement_index_entry.offset_,
                                               measurement_ie_end_offset))) {
  } else {
#if STORAGE_ENGIEN_DEBUG
    std::cout << "load timeseries index: " << timeseries_index_ << std::endl;
#endif
  }
  return ret;
}
#endif
} // end namespace storage
