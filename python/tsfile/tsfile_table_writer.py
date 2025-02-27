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

from tsfile import TableSchema, Tablet, TableNotExistError
from tsfile import TsFileWriter


class TsFileTableWriter():

    def __init__(self, path : str,  table_schema : TableSchema):
        self.writer = TsFileWriter(path)
        self.writer.register_table(table_schema)
        self.exclusive_table_name_ = table_schema.get_table_name()

    def write_table(self, tablet : Tablet):
        if tablet.get_target_name() == None:
            tablet.set_table_name(self.exclusive_table_name_)
        elif self.exclusive_table_name_ is not None and tablet.get_target_name() != self.exclusive_table_name_:
            raise TableNotExistError
        self.writer.write_table(tablet)

    def close(self):
        self.writer.close()

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()