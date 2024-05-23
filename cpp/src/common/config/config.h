#ifndef COMMON_CONFIG_CONFIG_H
#define COMMON_CONFIG_CONFIG_H

#include <stdint.h>

#include "common/mutex/mutex.h"
#include "utils/db_utils.h"
#include "utils/util_define.h"

namespace common {
enum ConfigLevel {
    INIT,     // Unchangeable, initialized during database init
    RESTART,  // Can be changed, but the database must be restarted to take
              // effect
    USERSET   // Session level update
};

typedef struct ConfigValue {
    uint32_t
        tsblock_mem_inc_step_size_;  // tsblock memory self-increment step size
    uint32_t tsblock_max_memory_;    // the maximum memory of a single tsblock
    const char *rest_service_ip_;
    int32_t rest_service_port_;
    WALFlushPolicy wal_flush_policy_;
    uint32_t seqtvlist_primary_array_size_;
    uint32_t seqtvlist_max_record_count_;
    uint32_t page_writer_max_point_num_;
    uint32_t page_writer_max_memory_bytes_;
    uint32_t max_degree_of_index_node_;
    double tsfile_index_bloom_filter_error_percent_;
    const char *tsfile_prefix_path_;
    TSEncoding time_encoding_type_;
    uint32_t memtable_flusher_poll_interval_seconds_;
} ConfigValue;

extern void init_config_value();

// In the future, configuration items need to be dynamically adjusted according
// to the level
extern void set_config_value();
extern void config_set_page_max_point_count(uint32_t page_max_ponint_count);
extern void config_set_max_degree_of_index_node(
    uint32_t max_degree_of_index_node);

// FORCE_INLINE bool wal_cfg_enabled() { return
// g_config_value_.wal_flush_policy_ != WAL_DISABLED; } FORCE_INLINE bool
// wal_cfg_should_wait_persisted() { return g_config_value_.wal_flush_policy_ >=
// WAL_FLUSH; }

}  // namespace common

#endif  // COMMON_CONFIG_CONFIG_H
