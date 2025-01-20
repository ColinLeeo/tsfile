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

#include "reader/block/device_ordered_tsblock_reader.h"

namespace storage {

bool DeviceOrderedTsBlockReader::has_next() { 
    if (current_reader_ != nullptr && !current_reader_->has_next()) {
        return false;
    }
    return true;
}

int DeviceOrderedTsBlockReader::next(common::TsBlock &ret_block) {
    int ret = common::E_OK;
    // if (!has_next()) {
    // }
    return ret;
}

}  // namespace storage
