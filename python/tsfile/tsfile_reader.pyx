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

#cython: language_level=3

from .tsfile_cpp cimport *
from .tsfile_py_cpp cimport *

from typing import List

from tsfile.schema import TSDataType as TSDataTypePy

cdef class ResultSetPy:
    """
    Get data from a query result.
    """
    cdef ResultSet result
    cdef public object metadata
    cdef public object device_name

    def __init__(self):
        pass

    cdef init_c_(self, ResultSet result, object device_name):
        """
        Init c symbols.
        """
        cdef ResultSetMetaData metadata_c
        self.result = result
        metadata_c = tsfile_result_set_get_metadata(self.result)
        self.metadata = from_c_result_set_meta_data(metadata_c)
        self.metadata.set_device_name(device_name)
        free_result_set_meta_data(metadata_c)

    def has_next(self):
        """
        Check if the query has next rows.
        """
        return tsfile_result_set_has_next(self.result)

    def next_block(self, max_row_num : str):
        pass

    def get_value_by_index(self, index : int):
        """
        Get value by index from query result set.
        """
        if tsfile_result_set_is_null_by_index(self.result, index):
            return None
        data_type = self.metadata.get_data_type(index)
        if data_type == TSDataTypePy.INT32:
            return tsfile_result_set_get_value_by_index_int32_t(self.result, index)
        elif data_type == TSDataTypePy.INT64:
            return tsfile_result_set_get_value_by_index_int64_t(self.result, index)
        elif data_type == TSDataTypePy.FLOAT:
            return tsfile_result_set_get_value_by_index_float(self.result, index)
        elif data_type == TSDataTypePy.DOUBLE:
            return tsfile_result_set_get_value_by_index_double(self.result, index)
        elif data_type == TSDataTypePy.BOOLEAN:
            return tsfile_result_set_get_value_by_index_bool(self.result, index)

    def get_value_by_name(self, column_name : str):
        """
        Get value by name from query result set.
        """
        if tsfile_result_set_is_null_by_name_c(self.result, column_name):
            return None
        ind = self.metadata.get_column_name_index(column_name)
        return self.get_value_by_index(ind)

    def is_null_by_index(self, index : int):
        """
        Checks whether the field at the specified index in the result set is null.

        This method queries the underlying result set to determine if the value
        at the given column index position represents a null value.
        """
        if index >= len(self.metadata.column_list) or index < 0:
            raise IndexError(
                f"Column index {index} out of range (column count: {self.metadata.column_num})"
            )
        return tsfile_result_set_is_null_by_index(self.result, index)

    def is_null_by_name(self, name : str):
        """
        Checks whether the field with the specified column name in the result set is null.
        """
        ind = self.metadata.get_column_name_index(name)
        return self.is_null_by_index(ind)

    def close(self):
        """
        Close result set, free C resource.
        :return:
        """
        free_tsfile_result_set(&self.result)

    def __dealloc__(self):
        self.close()

cdef class TsFileReaderPy:
    """
    Cython wrapper class for interacting with TsFileReader C implementation.

    Provides a Pythonic interface to read and query time series data from TsFiles.
    """
    cdef TsFileReader reader

    def __init__(self, pathname):
        """
        Initialize a TsFile reader for the specified file path.
        """
        self.init_reader(pathname)

    cdef init_reader(self, pathname):
        self.reader = tsfile_reader_new_c(pathname)

    def query_table(self, table_name : str, column_names : List[str],
              start_time : int = 0, end_time : int = 0) -> ResultSet:
        """
        Execute a time range query on specified table and columns.
        """
        cdef ResultSet result;
        result = tsfile_reader_query_table_c(self.reader, table_name, column_names, start_time, end_time)
        pyresult = ResultSetPy()
        pyresult.init_c_(result, table_name)
        return pyresult

    def query_timeseries(self, device_name : str, sensor_list : List[str], start_time : int = 0, end_time : int = 0) -> ResultSet:
        """
        Execute a time range query on specified path list.
        """
        cdef ResultSet result;
        result = tsfile_reader_query_paths_c(self.reader, device_name,  sensor_list, start_time, end_time)
        pyresult = ResultSetPy()
        pyresult.init_c_(result, device_name)
        return pyresult

    def close(self):
        """
        Close TsFile Reader.
        """
        cdef ErrorCode errcode
        errorcode = tsfile_reader_close(self.reader)
        check_error(errorcode)

    def __dealloc__(self):
        self.close()





