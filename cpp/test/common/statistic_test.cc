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
#include "common/statistic.h"

#include <gtest/gtest.h>

namespace storage {
TEST(BooleanStatisticTest, BasicFunctionality) {
    BooleanStatistic stat;
    EXPECT_EQ(stat.count_, 0);
    EXPECT_EQ(stat.start_time_, 0);
    EXPECT_EQ(stat.end_time_, 0);
    EXPECT_EQ(stat.sum_value_, 0);
    EXPECT_EQ(stat.first_value_, false);
    EXPECT_EQ(stat.last_value_, false);

    stat.update(1000, true);
    stat.update(2000, false);

    EXPECT_EQ(stat.count_, 2);
    EXPECT_EQ(stat.start_time_, 1000);
    EXPECT_EQ(stat.end_time_, 2000);
    EXPECT_EQ(stat.sum_value_, 1);
    EXPECT_EQ(stat.first_value_, true);
    EXPECT_EQ(stat.last_value_, false);

    common::ByteStream out(1024, common::MOD_STATISTIC_OBJ);
    stat.serialize_typed_stat(out);

    BooleanStatistic stat_deserialized;
    stat_deserialized.deserialize_typed_stat(out);

    EXPECT_EQ(stat_deserialized.sum_value_, stat.sum_value_);
    EXPECT_EQ(stat_deserialized.first_value_, stat.first_value_);
    EXPECT_EQ(stat_deserialized.last_value_, stat.last_value_);
}

TEST(Int32StatisticTest, BasicFunctionality) {
    Int32Statistic stat;

    EXPECT_EQ(stat.count_, 0);
    EXPECT_EQ(stat.start_time_, 0);
    EXPECT_EQ(stat.end_time_, 0);
    EXPECT_EQ(stat.sum_value_, 0);
    EXPECT_EQ(stat.min_value_, 0);
    EXPECT_EQ(stat.max_value_, 0);
    EXPECT_EQ(stat.first_value_, 0);
    EXPECT_EQ(stat.last_value_, 0);

    stat.update(1000, 10);
    stat.update(2000, 20);

    EXPECT_EQ(stat.count_, 2);
    EXPECT_EQ(stat.start_time_, 1000);
    EXPECT_EQ(stat.end_time_, 2000);
    EXPECT_EQ(stat.sum_value_, 30);
    EXPECT_EQ(stat.min_value_, 10);
    EXPECT_EQ(stat.max_value_, 20);
    EXPECT_EQ(stat.first_value_, 10);
    EXPECT_EQ(stat.last_value_, 20);

    common::ByteStream out(1024, common::MOD_STATISTIC_OBJ);
    stat.serialize_typed_stat(out);

    Int32Statistic stat_deserialized;
    stat_deserialized.deserialize_typed_stat(out);

    EXPECT_EQ(stat_deserialized.sum_value_, stat.sum_value_);
    EXPECT_EQ(stat_deserialized.min_value_, stat.min_value_);
    EXPECT_EQ(stat_deserialized.max_value_, stat.max_value_);
    EXPECT_EQ(stat_deserialized.first_value_, stat.first_value_);
    EXPECT_EQ(stat_deserialized.last_value_, stat.last_value_);
}

TEST(Int64StatisticTest, BasicFunctionality) {
    Int64Statistic stat;

    EXPECT_EQ(stat.count_, 0);
    EXPECT_EQ(stat.start_time_, 0);
    EXPECT_EQ(stat.end_time_, 0);
    EXPECT_EQ(stat.sum_value_, 0);
    EXPECT_EQ(stat.min_value_, 0);
    EXPECT_EQ(stat.max_value_, 0);
    EXPECT_EQ(stat.first_value_, 0);
    EXPECT_EQ(stat.last_value_, 0);

    stat.update(1000, 100);
    stat.update(2000, 200);

    EXPECT_EQ(stat.count_, 2);
    EXPECT_EQ(stat.start_time_, 1000);
    EXPECT_EQ(stat.end_time_, 2000);
    EXPECT_EQ(stat.sum_value_, 300);
    EXPECT_EQ(stat.min_value_, 100);
    EXPECT_EQ(stat.max_value_, 200);
    EXPECT_EQ(stat.first_value_, 100);
    EXPECT_EQ(stat.last_value_, 200);

    common::ByteStream out(1024, common::MOD_STATISTIC_OBJ);
    stat.serialize_typed_stat(out);

    Int64Statistic stat_deserialized;
    stat_deserialized.deserialize_typed_stat(out);

    EXPECT_EQ(stat_deserialized.sum_value_, stat.sum_value_);
    EXPECT_EQ(stat_deserialized.min_value_, stat.min_value_);
    EXPECT_EQ(stat_deserialized.max_value_, stat.max_value_);
    EXPECT_EQ(stat_deserialized.first_value_, stat.first_value_);
    EXPECT_EQ(stat_deserialized.last_value_, stat.last_value_);
}

TEST(FloatStatisticTest, BasicFunctionality) {
    FloatStatistic stat;

    EXPECT_EQ(stat.count_, 0);
    EXPECT_EQ(stat.start_time_, 0);
    EXPECT_EQ(stat.end_time_, 0);
    EXPECT_EQ(stat.sum_value_, 0);
    EXPECT_EQ(stat.min_value_, 0);
    EXPECT_EQ(stat.max_value_, 0);
    EXPECT_EQ(stat.first_value_, 0);
    EXPECT_EQ(stat.last_value_, 0);

    stat.update(1000, 10.5);
    stat.update(2000, 20.7);

    EXPECT_EQ(stat.count_, 2);
    EXPECT_EQ(stat.start_time_, 1000);
    EXPECT_EQ(stat.end_time_, 2000);
    EXPECT_FLOAT_EQ(stat.sum_value_, 31.2);
    EXPECT_FLOAT_EQ(stat.min_value_, 10.5);
    EXPECT_FLOAT_EQ(stat.max_value_, 20.7);
    EXPECT_FLOAT_EQ(stat.first_value_, 10.5);
    EXPECT_FLOAT_EQ(stat.last_value_, 20.7);

    common::ByteStream out(1024, common::MOD_STATISTIC_OBJ);
    stat.serialize_typed_stat(out);

    FloatStatistic stat_deserialized;
    stat_deserialized.deserialize_typed_stat(out);

    EXPECT_FLOAT_EQ(stat_deserialized.sum_value_, stat.sum_value_);
    EXPECT_FLOAT_EQ(stat_deserialized.min_value_, stat.min_value_);
    EXPECT_FLOAT_EQ(stat_deserialized.max_value_, stat.max_value_);
    EXPECT_FLOAT_EQ(stat_deserialized.first_value_, stat.first_value_);
    EXPECT_FLOAT_EQ(stat_deserialized.last_value_, stat.last_value_);
}

TEST(DoubleStatisticTest, BasicFunctionality) {
    DoubleStatistic stat;
    EXPECT_EQ(stat.count_, 0);
    EXPECT_EQ(stat.start_time_, 0);
    EXPECT_EQ(stat.end_time_, 0);
    EXPECT_EQ(stat.sum_value_, 0);
    EXPECT_EQ(stat.min_value_, 0);
    EXPECT_EQ(stat.max_value_, 0);
    EXPECT_EQ(stat.first_value_, 0);
    EXPECT_EQ(stat.last_value_, 0);

    stat.update(1000, 100.5);
    stat.update(2000, 200.7);

    EXPECT_EQ(stat.count_, 2);
    EXPECT_EQ(stat.start_time_, 1000);
    EXPECT_EQ(stat.end_time_, 2000);
    EXPECT_DOUBLE_EQ(stat.sum_value_, 301.2);
    EXPECT_DOUBLE_EQ(stat.min_value_, 100.5);
    EXPECT_DOUBLE_EQ(stat.max_value_, 200.7);
    EXPECT_DOUBLE_EQ(stat.first_value_, 100.5);
    EXPECT_DOUBLE_EQ(stat.last_value_, 200.7);

    common::ByteStream out(1024, common::MOD_STATISTIC_OBJ);
    stat.serialize_typed_stat(out);

    DoubleStatistic stat_deserialized;
    stat_deserialized.deserialize_typed_stat(out);

    EXPECT_DOUBLE_EQ(stat_deserialized.sum_value_, stat.sum_value_);
    EXPECT_DOUBLE_EQ(stat_deserialized.min_value_, stat.min_value_);
    EXPECT_DOUBLE_EQ(stat_deserialized.max_value_, stat.max_value_);
    EXPECT_DOUBLE_EQ(stat_deserialized.first_value_, stat.first_value_);
    EXPECT_DOUBLE_EQ(stat_deserialized.last_value_, stat.last_value_);
}

TEST(TimeStatisticTest, BasicFunctionality) {
    TimeStatistic stat;
    EXPECT_EQ(stat.count_, 0);
    EXPECT_EQ(stat.start_time_, 0);
    EXPECT_EQ(stat.end_time_, 0);

    stat.update(1000);
    stat.update(2000);

    EXPECT_EQ(stat.count_, 2);
    EXPECT_EQ(stat.start_time_, 1000);
    EXPECT_EQ(stat.end_time_, 2000);

    common::ByteStream out(1024, common::MOD_STATISTIC_OBJ);
    stat.serialize_typed_stat(out);

    TimeStatistic stat_deserialized;
    EXPECT_EQ(stat_deserialized.deserialize_typed_stat(out), common::E_OK);
    EXPECT_EQ(stat.count_, 2);
    EXPECT_EQ(stat.start_time_, 1000);
    EXPECT_EQ(stat.end_time_, 2000);
}

#if DEBUG_SE
TEST(StringStatisticTest, CppJavaGap) {
    Statistic* stat = StatisticFactory::alloc_statistic(common::STRING);
    char* array1 = new char[3]{'a', 'a', 'a'};
    char* array2 = new char[3]{'b', 'b', 'b'};
    common::String str1(array1, 3);
    common::String str2(array2, 3);
    stat->update(100, str1);
    stat->update(200, str2);

    common::ByteStream stream(1024, common::MOD_STATISTIC_OBJ);
    stat->serialize_to(stream);

    delete []array1;
    delete []array2;
    StatisticFactory::free(stat);

    auto buf_len = stream.total_size();
    char* buf = new char[buf_len];
    common::copy_bs_to_buf(stream, buf, buf_len);

    const ssize_t expected_size = 31;
    uint8_t expected_buf[expected_size] = {2, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0,
                                           0, 0, 0, 0, 200, 0, 0, 0, 3, 97, 97,
                                           97, 0, 0, 0, 3, 98, 98, 98};

    for (int i = 0; i < expected_size; i++) {
        EXPECT_EQ(buf[i], static_cast<char>(expected_buf[i]));
    }
}
#endif
} // namespace storage