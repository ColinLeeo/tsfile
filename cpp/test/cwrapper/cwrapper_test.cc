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
#include <utils/db_utils.h>
extern "C" {
#include "cwrapper/errno_define.h"
#include "cwrapper/tsfile_cwrapper.h"
}

#include "common/tablet.h"
#include "utils/errno_define.h"

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
    schema.table_name = strdup("testtable0");
    int id_schema_num = 5;
    int field_schema_num = 5;
    schema.column_num = column_num;
    schema.column_schemas =
        static_cast<ColumnSchema*>(malloc(column_num * sizeof(ColumnSchema)));
    for (int i = 0; i < id_schema_num; i++) {
        schema.column_schemas[i] =
            ColumnSchema{strdup(std::string("id" + std::to_string(i)).c_str()),
                         TS_DATATYPE_STRING, TS_COMPRESSION_UNCOMPRESSED,
                         TS_ENCODING_PLAIN, TAG};
    }
    int max_rows = 100;
    for (int i = 0; i < device_num; i++) {
        char* device_name = strdup(("device" + std::to_string(i)).c_str());
        char** measurements_name =
            static_cast<char**>(malloc(measurement_num * sizeof(char*)));
        TSDataType* data_types = static_cast<TSDataType*>(
            malloc(sizeof(TSDataType) * measurement_num));
        for (int j = 0; j < measurement_num; j++) {
            measurements_name[j] =
                strdup(("measurement" + std::to_string(j)).c_str());
            data_types[j] = TS_DATATYPE_INT64;
        }
        Tablet tablet =
            tablet_new_with_device(device_name, measurements_name, data_types,
                                   nullptr, measurement_num, max_rows);
        free(device_name);
        free(data_types);
        for (int j = 0; j < measurement_num; j++) {
            free(measurements_name[j]);
        }
        free(measurements_name);
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet_add_timestamp(tablet, row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet_add_value_by_index_int64_t(
                    tablet, row, j, static_cast<int64_t>(row + j));
            }
        }
    }
    delete[] literal;
    code = tsfile_writer_write(writer, tablet);
    ASSERT_EQ(code, RET_OK);
    ASSERT_EQ(tsfile_writer_close(writer), 0);


    TsFileReader reader =
        tsfile_reader_new("cwrapper_write_flush_and_read.tsfile", &code);
    ASSERT_EQ(code, 0);
    ResultSet result_set = tsfile_query_table(reader, schema.table_name,
                                              column_names, 10, 0, 100, &code);

    int row = 0;
    while (tsfile_result_set_next(result_set, &code) && code == RET_OK) {
        for (int i = 0; i < schema.column_num; i++) {
            char* ret = nullptr;
            switch (schema.column_schemas[i].data_type) {
                case TS_DATATYPE_STRING:
                    ret = tsfile_result_set_get_value_by_name_string(
                        result_set, schema.column_schemas[i].column_name);
                    ASSERT_EQ(std::string("device_id"), std::string(ret));
                    free(ret);
                    break;
                case TS_DATATYPE_INT64:
                    ASSERT_EQ(row, tsfile_result_set_get_value_by_name_int64_t(
                                       result_set,
                                       schema.column_schemas[i].column_name));
                    break;
                default:
                    break;
            }
        }
        for (int i = 7; i <= 11; i++) {
            ASSERT_EQ(row, tsfile_result_set_get_value_by_index_int64_t(
                               result_set, i));
        }
        row++;
    }
    ASSERT_EQ(row, num_timestamp);
    uint32_t size;
    TableSchema* all_schema =
        tsfile_reader_get_all_table_schemas(reader, &size);
    ASSERT_EQ(1, size);
    ASSERT_EQ(std::string(all_schema[0].table_name),
              std::string(schema.table_name));
    ASSERT_EQ(all_schema[0].column_num, schema.column_num);
    int count_int64_t = 0;
    int count_string = 0;
    for (int i = 0; i < column_num; i++) {
        if (all_schema[0].column_schemas[i].data_type == TS_DATATYPE_INT64) {
            count_int64_t++;
        } else if (all_schema[0].column_schemas[i].data_type ==
                   TS_DATATYPE_STRING) {
            count_string++;
        }
    }

    ResultSetMetaData metadata = tsfile_result_set_get_metadata(result_set);
    ASSERT_EQ(metadata.column_num, measurement_num);
    ASSERT_EQ(std::string(metadata.column_names[4]),
              std::string("device0.measurement4"));
    ASSERT_EQ(metadata.data_types[9], TS_DATATYPE_INT64);
    for (int i = 0; i < measurement_num - 1; i++) {
        ASSERT_TRUE(tsfile_result_set_next(result_set));
        ASSERT_FALSE(tsfile_result_set_is_null_by_index(result_set, i));
        ASSERT_EQ(tsfile_result_set_get_value_by_index_int64_t(result_set, i),
                  i * 2);
        ASSERT_EQ(tsfile_result_set_get_value_by_name_int64_t(
                      result_set,
                      std::string("measurement" + std::to_string(i)).c_str()),
                  i * 2);
    }
    free_tsfile_result_set(&result_set);
    free_result_set_meta_data(metadata);
    for (int i = 0; i < measurement_num; i++) {
        free(sensor_list[i]);
    }
    free(sensor_list);
    tsfile_reader_close(reader);
    free_tsfile_result_set(&result_set);
    free_table_schema(schema);
    free_table_schema(*all_schema);
    free(all_schema);
    for (int i = 0; i < column_num; i++) {
        free(column_names[i]);
    }
    free(column_names);
    free(data_types);
    free_write_file(&file);

}
}  // namespace cwrapper