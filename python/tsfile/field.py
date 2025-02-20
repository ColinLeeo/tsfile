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
from datetime import datetime

import numpy as np
import pandas as pd

from .constants import TSDataType
from .date_utils import parse_int_to_date


class Field(object):
    def __init__(self, field_name, data_type=None, value=None):
        """
        :param field_name: field's name
        :param data_type: TSDataType
        """
        self.field_name = field_name
        self.data_type = data_type
        self.value = value

    def get_data_type(self):
        return self.data_type

    def get_field_name(self):
        return self.field_name

    def is_null(self):
        return self.value is None

    def set_value(self, value):
        self.value = value

    def get_value(self):
        return self.value

    def get_bool_value(self):
        if self.value is None:
            return None
        if self.data_type is None:
            raise Exception("None Data Type Exception!")
        if self.data_type != TSDataType.BOOLEAN:
            raise TypeError(f"Expected BOOLEAN data type, got {self.data_type}.")

        if isinstance(self.value, bool):
            return self.value
        else:
            raise TypeError(f"Value is not a boolean: {type(self.value)}")

    def get_int_value(self):
        if self.value is None:
            return None
        if self.data_type is None:
            raise Exception("None Data Type Exception!")

        if (
            self.data_type != TSDataType.INT32
            or self.data_type != TSDataType.INT64
        ):
            raise TypeError(f"Expected INT32/64 data type, got {self.data_type}.")
        return np.int32(self.value)

    def get_long_value(self):
        if self.value is None:
            return None
        if self.data_type is None:
            raise Exception("None Data Type Exception!")

        if (
            self.data_type != TSDataType.INT32
            or self.data_type != TSDataType.INT64
        ):
            raise TypeError(f"Expected INT32/64 data type, got {self.data_type}.")

        return np.int64(self.value)

    def get_float_value(self):
        if self.value is None:
            return None
        if self.data_type is None:
            raise Exception("None Data Type Exception!")
        if (
            self.data_type != TSDataType.FLOAT
            or self.data_type != TSDataType.DOUBLE
        ):
            raise TypeError(f"Expected FLOAT/DOUBLE data type, got {self.data_type}.")
        return np.float32(self.value)

    def get_double_value(self):
        if self.value is None:
            return None
        if self.data_type is None:
            raise Exception("None Data Type Exception!")
        if (
            self.data_type != TSDataType.FLOAT
            or self.data_type != TSDataType.DOUBLE
        ):
            raise TypeError(f"Expected FLOAT/DOUBLE data type, got {self.data_type}.")
        return np.float64(self.value)

    def get_date_value(self):
        if self.value is None:
            return None
        if self.data_type is None:
            raise Exception("None Data Type Exception!")
        if self.data_type != TSDataType.DATE:
            raise TypeError(f"Expected DATE data type, got {self.data_type}.")
        if isinstance(self.value, datetime):
            return self.value
        elif isinstance(self.value, int):
            return parse_int_to_date(self.value)
        else:
            raise TypeError(f"Value is not a int or datetime: {type(self.value)}")


    def get_string_value(self):
        if self.value is None:
            return None
        if self.data_type is None:
            raise Exception("None Data Type Exception!")
        if (
                self.data_type != TSDataType.STRING or
                self.data_type != TSDataType.TEXT or
                self.data_type != TSDataType.BLOB
        ):
            raise TypeError(f"Expected STRING/TEXT/BLOB data type, got {self.data_type}.")

        # TEXT, STRING
        if self.data_type == TSDataType.TEXT or self.data_type == TSDataType.STRING:
            return self.value.decode("utf-8")
        # BLOB
        elif self.data_type == TSDataType.BLOB:
            return str(hex(int.from_bytes(self.value, byteorder="big")))
        else:
            return str(self.get_object_value(self.data_type))

    def __str__(self):
        return self.get_string_value()

    def get_object_value(self, data_type: TSDataType):
        """
        :param data_type: TSDataType
        """
        if self.data_type is None or self.value is None or self.value is pd.NA:
            return None
        if data_type == TSDataType.BOOLEAN:
            return bool(self.value)
        elif data_type == TSDataType.INT32:
            return np.int32(self.value)
        elif data_type == TSDataType.INT64 or data_type == TSDataType.TIMESTAMP:
            return np.int64(self.value)
        elif data_type == TSDataType.FLOAT:
            return np.float32(self.value)
        elif data_type == TSDataType.DOUBLE:
            return np.float64(self.value)
        elif data_type == TSDataType.DATE:
            return parse_int_to_date(self.value)
        elif data_type == TSDataType.TEXT or data_type == TSDataType.BLOB or data_type == TSDataType.STRING:
            return self.value
        else:
            raise RuntimeError("Unsupported data type:" + str(data_type))

    @staticmethod
    def get_field(field_name, value, data_type):
        """
        :param field_name: field's name
        :param value: field value corresponding to the data type
        :param data_type: TSDataType
        """
        if value is None or value is pd.NA:
            return None
        field = Field(field_name, data_type, value)
        return field
