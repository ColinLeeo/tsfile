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

"""Small synthetic ownership scenarios, each intended for a fresh interpreter."""

import gc
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from tsfile.exceptions import AlreadyExistsError
from tsfile import (
    ColumnCategory,
    ColumnSchema,
    DeviceSchema,
    Field,
    RowRecord,
    TableSchema,
    TimeseriesSchema,
    TSDataType,
    TsFileDataFrame,
    TsFileReader,
    TsFileTableWriter,
    TsFileWriter,
)


def run(case, directory):
    path = str(Path(directory) / "ownership.tsfile")
    if case == "schema_errors":
        with TsFileWriter(path) as writer:
            for _ in range(3):
                invalid = TimeseriesSchema("invalid", TSDataType.DOUBLE)
                invalid.encoding_type = None
                expect_error(
                    lambda: writer.register_timeseries("root.test.device", invalid)
                )
                device = DeviceSchema(
                    "root.test.device",
                    [TimeseriesSchema("valid", TSDataType.DOUBLE), invalid],
                )
                expect_error(lambda: writer.register_device(device))
                column = ColumnSchema("invalid", TSDataType.DOUBLE)
                column.category = None
                table = TableSchema(
                    "invalid", [ColumnSchema("valid", TSDataType.DOUBLE), column]
                )
                expect_error(lambda: writer.register_table(table))
            valid_device = DeviceSchema(
                "root.test.device", [TimeseriesSchema("value", TSDataType.DOUBLE)]
            )
            writer.register_device(valid_device)
            # Registration failure occurs after conversion, and must release
            # both the schema contents and its heap-allocated container.
            expect_error(
                lambda: writer.register_timeseries(
                    "root.test.device", TimeseriesSchema("value", TSDataType.DOUBLE)
                ),
                error_type=AlreadyExistsError,
            )
            expect_error(
                lambda: writer.register_device(valid_device),
                error_type=AlreadyExistsError,
            )
            valid_table = TableSchema(
                "valid", [ColumnSchema("value", TSDataType.DOUBLE)]
            )
            writer.register_table(valid_table)
            expect_error(
                lambda: writer.register_table(valid_table),
                error_type=AlreadyExistsError,
            )
        return
    if case == "tree":
        with TsFileWriter(path) as writer:
            writer.register_timeseries(
                "root.test.device", TimeseriesSchema("value", TSDataType.DOUBLE)
            )
            for i in range(32):
                writer.write_row_record(
                    RowRecord(
                        "root.test.device",
                        i,
                        [Field("value", float(i), TSDataType.DOUBLE)],
                    )
                )
        for _ in range(3):
            with TsFileDataFrame(path, show_progress=False) as frame:
                np.testing.assert_array_equal(frame[0][:], np.arange(32))
                assert frame.loc[0:31, [0]].shape == (32, 1)
        return
    schema = TableSchema(
        "ownership",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("text", TSDataType.STRING, ColumnCategory.FIELD),
            ColumnSchema("payload", TSDataType.BLOB, ColumnCategory.FIELD),
        ],
    )
    data = pd.DataFrame(
        {
            "time": [0, 1, 2],
            "device": ["sensor"] * 3,
            "value": [1.0, 2.0, 3.0],
            "text": ["hello", None, "world"],
            "payload": [b"abc", None, b"xyz"],
        }
    )
    with TsFileTableWriter(path, schema) as writer:
        writer.write_dataframe(data)
    for _ in range(3):
        with TsFileReader(path) as reader:
            if case == "arrow":
                with reader.query_table(
                    "ownership", ["value", "text", "payload"], batch_size=2
                ) as result:
                    batch = result.read_arrow_batch()
                # Arrow owns its buffers beyond the result and reader lifetime.
            else:
                try:
                    with reader.query_table(
                        "ownership", ["device", "value", "text", "payload"]
                    ) as result:
                        rows = 0
                        while result.next():
                            assert result.get_value_by_name("device") == "sensor"
                            assert result.get_value_by_name("value") == rows + 1
                            assert (
                                result.get_value_by_name("text") == data["text"][rows]
                            )
                            assert (
                                result.get_value_by_name("payload")
                                == data["payload"][rows]
                            )
                            rows += 1
                            if case == "early":
                                break
                            if case == "exception":
                                raise RuntimeError("consumer interrupted")
                except RuntimeError as exc:
                    if case != "exception":
                        raise
                    assert str(exc) == "consumer interrupted"
                    assert reader.get_all_table_schemas()
        if case == "arrow":
            assert batch.column("value").to_pylist() == [1.0, 2.0]
            del batch
    with TsFileDataFrame(path, show_progress=False) as frame:
        np.testing.assert_array_equal(frame[0][:], [1.0, 2.0, 3.0])
    gc.collect()


def expect_error(action, error_type=ValueError):
    try:
        action()
    except error_type:
        return
    raise AssertionError("Invalid schema unexpectedly registered")


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2])
