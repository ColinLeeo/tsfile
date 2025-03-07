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

#include "common/db_common.h"
#include "common/path.h"
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/expression.h"
#include "reader/filter/filter.h"
#include "reader/qds_with_timegenerator.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

#define HANDLE_ERROR(err_no)                  \
    do {                                      \
        if (err_no != 0) {                    \
            printf("get err no: %d", err_no); \
            return err_no;                    \
        }                                     \
    } while (0)

int demo_read();
int demo_write();
