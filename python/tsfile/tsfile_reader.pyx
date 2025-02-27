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

import weakref

import pandas as pd
from pandas import DataFrame



from .tsfile_cpp cimport *
from .tsfile_py_cpp cimport *
cimport cython

from typing import List

from tsfile.schema import TSDataType as TSDataTypePy

cdef class ResultSetPy:
    """
    Get data from a query result.
    """
    __pyx_allow_weakref__ = True
    cdef ResultSet result
    cdef object metadata
    cdef object device_name
    cdef object not_invalid_result_set
    cdef object tsfile_reader
    cdef object __weakref__


    def __init__(self, tsfile_reader : TsFileReaderPy):
        self.metadata = None
        self.device_name = None
        self.not_invalid_result_set = False
        self.tsfile_reader = weakref.ref(tsfile_reader)
        pass

    cdef init_c(self, ResultSet result, object device_name):
        """
        Init c symbols.
        """
        cdef ResultSetMetaData metadata_c
        self.result = result
        metadata_c = tsfile_result_set_get_metadata(self.result)
        self.metadata = from_c_result_set_meta_data(metadata_c)
        self.metadata.set_device_name(device_name)
        free_result_set_meta_data(metadata_c)

    def next(self):
        """
        Check if the query has next rows.
        """
        cdef ErrorCode code = 0
        self.check_result_set_invalid()
        has_next =  tsfile_result_set_next(self.result, &code)
        check_error(code)
        return has_next


    def get_result_column_info(self):
        return {
            column_name:column_type
            for column_name, column_type in zip(
                self.metadata.column_list,
                self.metadata.data_types
            )
        }

    def read_next_data_frame(self, max_row_num : int = 1024):
        """
        :param max_row_num:
        :return: a dataframe contains data from query result.
        """
        self.check_result_set_invalid()
        column_names = self.metadata.get_column_list()
        column_num = self.metadata.get_column_num()
        data_type = [self.metadata.get_data_type(i).to_pandas_dtype() for i in range(column_num)]

        data_container = {
            column_name: [] for column_name in column_names
        }

        cur_line = 0
        while self.next() and cur_line < max_row_num:
            row_data = (
                self.get_value_by_index(i)
                for i in range(column_num)
            )
            for column_name, value in zip(column_names, row_data):
                data_container[column_name].append(value)

        df = pd.DataFrame(data_container)
        data_type_dict = {col: dtype for col, dtype in zip(column_names, data_type)}
        return df.astype(data_type_dict)

    def get_value_by_index(self, index : int):
        """
        Get value by index from query result set.
        """
        self.check_result_set_invalid()
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
        self.check_result_set_invalid()
        if tsfile_result_set_is_null_by_name_c(self.result, column_name):
            return None
        ind = self.metadata.get_column_name_index(column_name)
        return self.get_value_by_index(ind + 1)

    def is_null_by_index(self, index : int):
        """
        Checks whether the field at the specified index in the result set is null.

        This method queries the underlying result set to determine if the value
        at the given column index position represents a null value.
        """
        self.check_result_set_invalid()
        if index > len(self.metadata.column_list) or index < 1:
            raise IndexError(
                f"Column index {index} out of range (column count: {len(self.metadata.column_list)})"
            )
        return tsfile_result_set_is_null_by_index(self.result, index)

    def is_null_by_name(self, name : str):
        """
        Checks whether the field with the specified column name in the result set is null.
        """
        self.check_result_set_invalid()
        ind = self.metadata.get_column_name_index(name)
        return self.is_null_by_index(ind + 1)

    def check_result_set_invalid(self):
        if self.not_invalid_result_set:
            raise Exception("Invalid result set. TsFile Reader not exists")

    def get_result_set_invalid(self):
        return self.not_invalid_result_set

    def close(self):
        """
        Close result set, free C resource.
        :return:
        """
        if self.result != NULL:
            free_tsfile_result_set(&self.result)


        if self.tsfile_reader is not None:
            reader = self.tsfile_reader()
            if reader is not None:
                reader.notify_result_set_discard(self)

        self.result = NULL
        self.not_invalid_result_set = True

    def set_invalid_result_set(self, invalid : bool):
        self.not_invalid_result_set = invalid
        self.close()

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()



cdef class TsFileReaderPy:
    """
    Cython wrapper class for interacting with TsFileReader C implementation.

    Provides a Pythonic interface to read and query time series data from TsFiles.
    """
    cdef TsFileReader reader
    cdef object activate_result_set_list
    __pyx_allow_weakref__ = True
    cdef object __weakref__


    def __init__(self, pathname):
        """
        Initialize a TsFile reader for the specified file path.
        """
        self.init_reader(pathname)
        self.activate_result_set_list = weakref.WeakSet()

    cdef init_reader(self, pathname):
        self.reader = tsfile_reader_new_c(pathname)

    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = 0, end_time : int = 0) -> ResultSetPy:
        """
        Execute a time range query on specified table and columns.
        """
        cdef ResultSet result;
        result = tsfile_reader_query_table_c(self.reader, table_name, column_names, start_time, end_time)
        pyresult = ResultSetPy(self)
        pyresult.init_c(result, table_name)
        self.activate_result_set_list.add(pyresult)
        return pyresult

    def query_timeseries(self, device_name : str, sensor_list : List[str], start_time : int = 0,
                         end_time : int = 0) -> ResultSetPy:
        """
        Execute a time range query on specified path list.
        """
        cdef ResultSet result;
        result = tsfile_reader_query_paths_c(self.reader, device_name, sensor_list, start_time, end_time)
        pyresult = ResultSetPy(self)
        pyresult.init_c(result, device_name)
        self.activate_result_set_list.add(pyresult)
        return pyresult

    def notify_result_set_discard(self, result_set: ResultSetPy):
        self.activate_result_set_list.discard(result_set)

    def close(self):
        """
        Close TsFile Reader, if reader has result sets, invalid them.
        """
        if self.reader == NULL:
            return
        # result_set_bak to avoid runtime error.
        result_set_bak = list(self.activate_result_set_list)
        for result_set in result_set_bak:
            result_set.set_invalid_result_set(True)

        cdef ErrorCode err_code
        err_code = tsfile_reader_close(self.reader)
        check_error(err_code)
        self.reader = NULL

    def get_active_query_result(self):
        return self.activate_result_set_list

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
