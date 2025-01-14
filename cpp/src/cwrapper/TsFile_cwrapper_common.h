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


typedef enum {
    TS_TYPE_INT32,
    TS_TYPE_BOOLEAN,
    TS_TYPE_FLOAT,
    TS_TYPE_DOUBLE,
    TS_INT64,
    TS_TYPE_TEXT
} TSDataType;

typedef enum {
    TS_ENCODING_PLAIN,
    TS_ENCODING_TS_DIFF,
    TS_ENCODING_DICTIONARY,
    TS_ENCODING_RLE,
    TS_ENCODING_BITMAP,
    TS_ENCODING_GORILLA_V1,
    TS_ENCODING_REGULAR,
    TS_ENCODING_GORILLA,
    TS_ENCODING_ZIGZAG,
    TS_ENCODING_FREQ
} TSEncoding;

typedef enum {
    TS_COMPRESS_UNCOMPRESS,
    TS_COMPRESS_SNAPPY,
    TS_COMPRESS_GZIP,
    TS_COMPRESS_LZO,
    TS_COMPRESS_SDT,
    TS_COMPRESS_PAA,
    TS_COMPRESS_PLA,
    TS_COMPRESS_LZ4
} CompressionType;

typedef struct column_category {

} ColumnCategory;