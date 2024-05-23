#ifndef COMMON_CONTAINER_HASH_FUNC_H
#define COMMON_CONTAINER_HASH_FUNC_H

#include <stddef.h>
#include <stdint.h>

#include "common/container/hash_table.h"
#include "common/container/murmur_hash3.h"
#include "common/container/slice.h"
#include "utils/db_utils.h"

#define TSID_MAX_LEN 20

namespace common {

/*
 * using djb2 hash algorithm
 */
struct StringHashFunc {
    uint32_t hash_fmix32(uint32_t h) {
        h ^= h >> 16;
        h *= 0x3243f6a9U;
        h ^= h >> 16;
        return h;
    }

    uint32_t operator()(const void *data) {
        uint32_t hash_ = (const uint32_t)5381;
        const char *str = (const char *)data;
        char c;
        while ((c = *str++)) {
            hash_ = ((hash_ << 5) + hash_) + c;
        }
        return hash_fmix32(hash_);
    }
};

/*
 * using djb2 hash algorithm
 */
struct SliceHashFunc {
    uint32_t hash_fmix32(uint32_t h) {
        h ^= h >> 16;
        h *= 0x3243f6a9U;
        h ^= h >> 16;
        return h;
    }

    uint32_t operator()(const Slice &slice) {
        uint32_t hash_ = (const uint32_t)5381;
        const char *str = (const char *)slice.data();
        char c;
        while ((c = *str++)) {
            hash_ = ((hash_ << 5) + hash_) + c;
        }
        return hash_fmix32(hash_);
    }
};

/*
 * using murmur_hash hash algorithm
 */
struct TsIDHashFunc {
    uint32_t hash_fmix32(uint32_t h) {
        h ^= h >> 16;
        h *= 0x3243f6a9U;
        h ^= h >> 16;
        return h;
    }

    uint32_t operator()(const TsID &data) {
        int32_t tmp = data.db_nid_ * 10000 + data.device_nid_ * 100 +
                      data.measurement_nid_;
        return Murmur128Hash::hash(tmp, 0);
    }
};

/*
 * using murmur_hash hash algorithm
 */
struct NodeIDHashFunc {
    uint32_t operator()(const NodeID &data) {
        return Murmur128Hash::hash(static_cast<int32_t>(data), 0);
    }
};

}  // end namespace common
#endif  // COMMON_CONTAINER_HASH_FUNC_H
