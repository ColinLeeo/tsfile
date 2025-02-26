/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include <gtest/gtest.h>
#include <unistd.h>
extern "C" {
#include "cwrapper/errno_define.h"
#include "cwrapper/tsfile_cwrapper.h"
}

#include "utils/errno_define.h"

using namespace common;

namespace cwrapper {
class CWrapperTest : public testing::Test {};

// TEST_F(CWrapperTest, RegisterTimeSeries) {
//     ERRNO code = 0;
//     char* temperature = strdup("temperature");
//     TimeseriesSchema ts_schema{temperature, TS_DATATYPE_INT32,
//                                TS_ENCODING_PLAIN,
//                                TS_COMPRESSION_UNCOMPRESSED};
//     remove("cwrapper_register_timeseries.tsfile");
//     TsFileWriter writer =
//     tsfile_writer_new("cwrapper_register_timeseries.tsfile", &code);
//     ASSERT_EQ(code, 0);
//     code = tsfile_writer_register_timeseries(writer, "device1", &ts_schema);
//     ASSERT_EQ(code, 0);
//     free(temperature);
//     tsfile_writer_close(writer);
// }

TEST_F(CWrapperTest, WriterFlushTabletAndReadData) {
    ERRNO code = 0;
    const int column_num = 10;
    remove("cwrapper_write_flush_and_read.tsfile");
    TableSchema schema;
    schema.table_name = "table1";
    schema.column_num = column_num;
    schema.column_schemas =
        static_cast<ColumnSchema*>(malloc(column_num * sizeof(ColumnSchema)));
    schema.column_schemas[0] =
        ColumnSchema{"id1", TS_DATATYPE_STRING, TS_COMPRESSION_UNCOMPRESSED,
                     TS_ENCODING_PLAIN, TAG};
    schema.column_schemas[1] =
        ColumnSchema{"id2", TS_DATATYPE_STRING, TS_COMPRESSION_UNCOMPRESSED,
                     TS_ENCODING_PLAIN, TAG};
    for (int i = 2; i < column_num; i++) {
        schema.column_schemas[i] = ColumnSchema{
            strdup(("s" + std::to_string(i)).c_str()), TS_DATATYPE_INT32,
            TS_COMPRESSION_UNCOMPRESSED, TS_ENCODING_PLAIN, FIELD};
    }
    TsFileWriter writer = tsfile_writer_new(
        "cwrapper_write_flush_and_read.tsfile", &schema, &code);
    ASSERT_EQ(code, RET_OK);

    for (int i = 2; i < column_num; i++) {
        free(schema.column_schemas[i].column_name);
    }
    free(schema.column_schemas);


    int max_rows = 100;
    char** column_names =
        static_cast<char**>(malloc(column_num * sizeof(char*)));
    TSDataType* data_types =
        static_cast<TSDataType*>(malloc(sizeof(TSDataType) * column_num));

    column_names[0] = strdup(std::string("id1").c_str());
    column_names[1] = strdup(std::string("id2").c_str());
    for (int i = 2; i < column_num; i++) {
        column_names[i] = strdup(("s" + std::to_string(i)).c_str());
        data_types[i] = TS_DATATYPE_INT32;
    }
    data_types[0] = TS_DATATYPE_STRING;
    data_types[1] = TS_DATATYPE_STRING;
    Tablet tablet = tablet_new(column_names, data_types, column_num, max_rows);
    for(int i = 2; i < column_num; i++) {
        free(column_names[i]);
    }
    free(column_names);

    for (int i = 0; i < max_rows; i++) {
        code = tablet_add_timestamp(tablet, i, static_cast<Timestamp>(i * 10));
        ASSERT_EQ(code, 0);
        code = tablet_add_value_by_index_string(tablet, i, 0, "device");
        ASSERT_EQ(code, 0);
        code = tablet_add_value_by_index_string(
            tablet, i, 1, std::string("sensor" + std::to_string(i)).c_str());
        ASSERT_EQ(code, 0);
        for (int j = 2; j < column_num; j++) {
            code = tablet_add_value_by_index_int32_t(
                tablet, i, j, static_cast<int32_t>(i * 5));
            ASSERT_EQ(code, 0);
        }
    }
    code = tsfile_writer_write(writer, tablet);
    ASSERT_EQ(code, RET_OK);
    ASSERT_EQ(tsfile_writer_close(writer), 0);

    TsFileReader reader =
        tsfile_reader_new("cwrapper_write_flush_and_read.tsfile", &code);
    ASSERT_EQ(code, 0);

    char** sensor_list = static_cast<char**>(malloc(4 * sizeof(char*)));
    sensor_list[0] = "id1";
    sensor_list[1] = "id2";
    sensor_list[2] = "s3";
    sensor_list[3] = "s4";
    ResultSet result_set =
        tsfile_query_table(reader, "table1", sensor_list, 4, 0, 100, &code);

    ResultSetMetaData metadata = tsfile_result_set_get_metadata(result_set);
    ASSERT_EQ(metadata.column_num, 4);
    ASSERT_EQ(std::string(metadata.column_names[3]),
              std::string("s4"));
    ASSERT_EQ(metadata.data_types[3], TS_DATATYPE_INT32);
    while(tsfile_result_set_next(result_set, &code) && code == E_OK) {
        std::cout<< tsfile_result_set_is_null_by_index(result_set, 1);
    }


}
}  // namespace cwrapper