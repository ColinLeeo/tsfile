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

#include <malloc.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "c_examples.h"

ERRNO write_tsfile() {
    ERRNO code = 0;
    char* table_name = "table1";

    // Create tsfile writer with specify path.
    TsFileWriter writer = tsfile_writer_new("test.tsfile", &code);
    HANDLE_ERROR(code);

    // Table schema.
    TableSchema table_schema = {
        .table_name = table_name,
        .column_schemas =
            (ColumnSchema[]){ColumnSchema{"id1", TS_DATATYPE_TEXT, TAG},
                             ColumnSchema{"id2", TS_DATATYPE_TEXT, TAG},
                             ColumnSchema{"s1", TS_DATATYPE_INT32, FIELD}}};

    // Register a table with tsfile writer.
    code = tsfile_writer_register_table(writer, &table_schema);
    HANDLE_ERROR(code);

    // Create tablet to insert data.
    Tablet tablet = tablet_new_with_device(
        table_name, (char*[]){"id1", "id2", "s1"},
        (TSDataType[]){TS_DATATYPE_TEXT, TS_DATATYPE_TEXT, TS_DATATYPE_INT32},
        3, 1024);

    for (int row = 0; row < 5; row++) {
        Timestamp timestamp = row;
        tablet_add_timestamp(tablet, row, timestamp);
        tablet_add_value_by_name_string(tablet, row, "id1", "id_field_1");
        tablet_add_value_by_name_string(tablet, row, "id2", "id_field_2");
        tablet_add_value_by_name_int32_t(tablet, row, "s1", row);
    }

    // Write tablet data.
    HANDLE_ERROR(tsfile_writer_write_tablet(writer, tablet));

    // Flush tablet data.
    HANDLE_ERROR(tsfile_writer_flush_data(writer));

    // Close writer.
    HANDLE_ERROR(tsfile_writer_close(writer));

}