# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import pytest

import os

from tsfile import TsFileWriter, TsFileReader
from tsfile import TimeseriesSchema, DeviceSchema
from tsfile import TSDataType
from tsfile import Tablet, RowRecord, Field

def test_row_record_write_and_read():
    try:
        writer = TsFileWriter("record_write_and_read.tsfile")
        timeseries = TimeseriesSchema("level1", TSDataType.INT64)
        writer.register_timeseries("root.device1", timeseries)
        writer.register_timeseries("root.device1", TimeseriesSchema("level2", TSDataType.DOUBLE))
        writer.register_timeseries("root.device1", TimeseriesSchema("level3", TSDataType.INT32))

        max_row_num = 1000
        for i in range(max_row_num):
            row = RowRecord("root.device1", i,
                            [Field("level1", i, TSDataType.INT64),
                                    Field("level2",i*1.1, TSDataType.DOUBLE),
                                    Field("level3", i*2, TSDataType.INT32)])
            writer.write_row_record(row)

        writer.close()

        reader = TsFileReader("record_write_and_read.tsfile")
        result = reader.query_timeseries("root.device1", ["level1","level2"], 10, 100)
        i = 10
        while result.next():
            assert result.get_value_by_index(0) == i
            assert result.get_value_by_name("level2") == i * 1.1
            i = i + 1
        print(reader.get_active_query_result())
        result.close()
        print(reader.get_active_query_result())
        reader.close()
    finally:
        if os.path.exists("record_write_and_read.tsfile"):
            os.remove("record_write_and_read.tsfile")

def test_tablet_write_and_read():
    try:
        writer = TsFileWriter("tablet_write_and_read.tsfile")
        measurement_num = 30
        for i in range(measurement_num):
            writer.register_timeseries("root.device1", TimeseriesSchema('level' + str(i), TSDataType.INT64))

        max_row_num = 10000
        tablet_row_num = 1000
        tablet_num = 0
        for i in range(max_row_num // tablet_row_num):
            tablet = Tablet("root.device1",[f'level{j}' for j in range(measurement_num)],[TSDataType.INT64 for _ in range(measurement_num)], tablet_row_num)
            for row in range(tablet_row_num):
                tablet.add_timestamp(row, row + tablet_num * tablet_row_num)
                for col in range(measurement_num):
                    tablet.add_value_by_index(col, row, row + tablet_num * tablet_row_num)
            writer.write_tablet(tablet)
            tablet_num += 1

        writer.close()

        reader = TsFileReader("tablet_write_and_read.tsfile")
        result = reader.query_timeseries("root.device1", ["level0"], 0, 1000000)
        row_num = 0
        while result.next():
            assert result.is_null_by_index(0) == False
            assert result.get_value_by_name("level0") == row_num
            row_num = row_num + 1

        assert row_num == max_row_num
        reader.close()
        with pytest.raises(Exception):
            result.next()

    finally:
        if os.path.exists("tablet_write_and_read.tsfile"):
            os.remove("tablet_write_and_read.tsfile")


