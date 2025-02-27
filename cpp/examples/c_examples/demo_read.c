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

#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>

#include "c_examples.h"

// This example shows you how to read tsfile.
ERRNO read_tsfile() {
    ERRNO code = 0;
    char* table_name = "table1";

    // Create tsfile reader with specify tsfile's path
    TsFileReader reader = tsfile_reader_new("test_c.tsfile", &code);
    HANDLE_ERROR(code);

    ResultSet ret =
        tsfile_query_table(reader, table_name, (char*[]){"id1", "id2", "s1"}, 3, 0, 10, &code);

    // Get query result metadata: column name and datatype
    ResultSetMetaData metadata = tsfile_result_set_get_metadata(ret);
    int sensor_num = metadata.column_num;

    for (int i = 0; i < sensor_num; i++) {
        printf("column:%s, datatype:%d\n", metadata.column_names[i],
               metadata.data_types[i]);
    }

    // Get data by column name or index.
    while (tsfile_result_set_next(ret, &code) && code == RET_OK) {
        Timestamp timestamp = tsfile_result_set_get_value_by_index_int64_t(ret, 1);
        printf("%ld ", timestamp);
        for (int i = 1; i < sensor_num; i++) {
            if (tsfile_result_set_is_null_by_index(ret, i)) {
                printf("null ");
            } else {
                switch (metadata.data_types[i]) {
                    case TS_DATATYPE_BOOLEAN:
                        printf("%d", tsfile_result_set_get_value_by_index_bool(ret, i));
                    break;
                    case TS_DATATYPE_INT32:
                        printf("%d", tsfile_result_set_get_value_by_index_int32_t(ret, i));
                    break;
                    case TS_DATATYPE_INT64:
                        // 注意：int64_t 应使用 %lld（Linux）或 %I64d（Windows）
                            printf("%lld", tsfile_result_set_get_value_by_index_int64_t(ret, i));
                    break;
                    case TS_DATATYPE_FLOAT:
                        printf("%f", tsfile_result_set_get_value_by_index_float(ret, i));
                    break;
                    case TS_DATATYPE_DOUBLE:
                        printf("%lf", tsfile_result_set_get_value_by_index_double(ret, i));
                    break;
                    case TS_DATATYPE_STRING:
                        printf("%s", tsfile_result_set_get_value_by_index_string(ret, i));
                    break;
                    default:
                        printf("unknown_type");
                    break;
                }
            }
        }
    }

    free_result_set_meta_data(metadata);
    free_tsfile_result_set(ret);
    return 0;
}
