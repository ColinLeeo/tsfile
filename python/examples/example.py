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
from Cython.Includes.cpython.time import result

from tsfile import TsFileWriter, TsFileReader, TSDataType, TSEncoding, Compressor
from tsfile import DeviceSchema, TimeseriesSchema, RowRecord, Field
from tsfile import ResultSet
import os

## tsfile path.
data_dir = os.path.join(os.path.dirname(__file__), "test.tsfile")
if os.path.exists(data_dir):
    os.remove(data_dir)

## Tree Model Write Data

DEVICE_NAME = "root.device"

writer = TsFileWriter(data_dir)

timeseries = TimeseriesSchema("temp1", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED)
timeseries2 = TimeseriesSchema("temp2", TSDataType.INT64)
timeseries3 = TimeseriesSchema("level1", TSDataType.BOOLEAN)

### register timeseries
writer.register_timeseries(DEVICE_NAME, timeseries)

### register device
device = DeviceSchema(DEVICE_NAME, [timeseries2, timeseries3])
writer.register_device(device)

### Write data with row record
row_num = 10
for i in range(row_num):
    row_record = RowRecord(DEVICE_NAME, i+1,
                           [Field("temp1", TSDataType.INT32, i),
                            Field("temp2", TSDataType.INT64, i)])
    writer.write_row_record(row_record)

### Flush data and close writer.
writer.close()

## Tree Model Read Data

reader = TsFileReader(data_dir)

### Query device with specify time scope
result = reader.query_timeseries(DEVICE_NAME, ["temp1", "temp2"], 0, 100)

### Get result list data types
sensor_info_list = result.get_result_column_info()
print(sensor_info_list)

### Print data
while result.next():
    print(result.get_value_by_name("temp1"))
    print(result.get_value_by_index(1))
result.close()

### Get query result which can free automatically

with reader.query_timeseries(DEVICE_NAME, ["level1"], 0, 100) as result:
    while result.next():
        print(result.get_value_by_name("level1"))

reader.close()

## Table Model Read and Write


device = DeviceSchema(DEVICE_NAME, [timeseries, timeseries2])
writer.register_device(device)

rc = RowRecord(DEVICE_NAME, 100, [Field("temp1", TSDataType.INT32, 10), Field("temp2", TSDataType.INT64, 10)])
writer.write_row_record(rc)
writer.close()

reader = TsFileReader(data_dir)
sensor_name = ["temp1", "temp2"]
result = reader.query_timeseries("root.device", sensor_name, 0, 110)
while result.next():
    print(result.get_value_by_index(0))
    print(result.get_value_by_index(1))
    print(result.get_value_by_name("temp1"))
    print(result.get_value_by_name("temp2"))
