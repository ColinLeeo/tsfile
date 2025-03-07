/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package org.apache.tsfile.file.metadata.statistics;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.filter.StatisticsClassException;
import org.apache.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * This class is used for recording statistic information of each measurement in a delta file. While
 * writing processing, the processor records the statistics information. Statistics includes
 * maximum, minimum and null value count up to version 0.0.1.<br>
 * Each data type extends this Statistic as super class.<br>
 * <br>
 * For the statistics in the Unseq file TimeSeriesMetadata, only firstValue, lastValue, startTime
 * and endTime can be used.</br>
 */
public abstract class Statistics<T extends Serializable> {

  private static final Logger LOG = LoggerFactory.getLogger(Statistics.class);

  /**
   * isEmpty being false means this statistic has been initialized and the max and min is not null.
   */
  protected boolean isEmpty = true;

  /** number of time-value points. */
  private int count = 0;

  private long startTime = Long.MAX_VALUE;
  private long endTime = Long.MIN_VALUE;

  static final String STATS_UNSUPPORTED_MSG = "%s statistics does not support: %s";

  /**
   * static method providing statistic instance for respective data type.
   *
   * @param type - data type
   * @return Statistics
   * @throws UnknownColumnTypeException if the type is unknown
   */
  public static Statistics<? extends Serializable> getStatsByType(TSDataType type) {
    switch (type) {
      case INT32:
        return new IntegerStatistics();
      case INT64:
        return new LongStatistics();
      case TEXT:
        return new BinaryStatistics();
      case BOOLEAN:
        return new BooleanStatistics();
      case DOUBLE:
        return new DoubleStatistics();
      case FLOAT:
        return new FloatStatistics();
      case VECTOR:
        return new TimeStatistics();
      case DATE:
        return new DateStatistics();
      case TIMESTAMP:
        return new TimestampStatistics();
      case STRING:
        return new StringStatistics();
      case BLOB:
        return new BlobStatistics();
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public static long getSizeByType(TSDataType type) {
    switch (type) {
      case INT32:
        return IntegerStatistics.INSTANCE_SIZE;
      case INT64:
        return LongStatistics.INSTANCE_SIZE;
      case TEXT:
        return BinaryStatistics.INSTANCE_SIZE;
      case BOOLEAN:
        return BooleanStatistics.INSTANCE_SIZE;
      case DOUBLE:
        return DoubleStatistics.INSTANCE_SIZE;
      case FLOAT:
        return FloatStatistics.INSTANCE_SIZE;
      case VECTOR:
        return TimeStatistics.INSTANCE_SIZE;
      case DATE:
        return DateStatistics.INSTANCE_SIZE;
      case TIMESTAMP:
        return TimestampStatistics.INSTANCE_SIZE;
      case STRING:
        return StringStatistics.INSTANCE_SIZE;
      case BLOB:
        return BlobStatistics.INSTANCE_SIZE;
      default:
        throw new UnknownColumnTypeException(type.toString());
    }
  }

  public abstract TSDataType getType();

  public int getSerializedSize() {
    return ReadWriteForEncodingUtils.uVarIntSize(count) // count
        + 16 // startTime, endTime
        + getStatsSize();
  }

  public abstract int getStatsSize();

  public abstract long getRetainedSizeInBytes();

  public int serialize(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(count, outputStream);
    byteLen += ReadWriteIOUtils.write(startTime, outputStream);
    byteLen += ReadWriteIOUtils.write(endTime, outputStream);
    // value statistics of different data type
    byteLen += serializeStats(outputStream);
    return byteLen;
  }

  abstract int serializeStats(OutputStream outputStream) throws IOException;

  /**
   * deserialize data from the inputStream.
   *
   * @param inputStream input stream
   * @throws IOException exception when operating stream
   */
  public abstract void deserialize(InputStream inputStream) throws IOException;

  public abstract void deserialize(ByteBuffer byteBuffer);

  public static Statistics<? extends Serializable> deserialize(
      InputStream inputStream, TSDataType dataType) throws IOException {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream));
    statistics.setStartTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setEndTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.deserialize(inputStream);
    statistics.isEmpty = false;
    return statistics;
  }

  public static Statistics<? extends Serializable> deserialize(
      ByteBuffer buffer, TSDataType dataType) {
    Statistics<? extends Serializable> statistics = getStatsByType(dataType);
    statistics.setCount(ReadWriteForEncodingUtils.readUnsignedVarInt(buffer));
    statistics.setStartTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setEndTime(ReadWriteIOUtils.readLong(buffer));
    statistics.deserialize(buffer);
    statistics.isEmpty = false;
    return statistics;
  }

  public abstract T getMinValue();

  public abstract T getMaxValue();

  public abstract T getFirstValue();

  public abstract T getLastValue();

  public abstract double getSumDoubleValue();

  public abstract long getSumLongValue();

  /**
   * merge parameter to this statistic.
   *
   * @throws StatisticsClassException cannot merge statistics
   */
  @SuppressWarnings("unchecked")
  public void mergeStatistics(Statistics<? extends Serializable> stats) {
    if (this.getClass() == stats.getClass() || canMerge(stats.getType(), this.getType())) {
      if (!stats.isEmpty) {
        if (stats.startTime < this.startTime) {
          this.startTime = stats.startTime;
        }
        if (stats.endTime > this.endTime) {
          this.endTime = stats.endTime;
        }
        // must be sure no overlap between two statistics
        this.count += stats.count;
        mergeStatisticsValue(stats);
        isEmpty = false;
      }
    } else {
      Class<?> thisClass = this.getClass();
      Class<?> statsClass = stats.getClass();
      LOG.warn("Statistics classes mismatched,no merge: {} v.s. {}", thisClass, statsClass);

      throw new StatisticsClassException(thisClass, statsClass);
    }
  }

  public static boolean canMerge(TSDataType from, TSDataType to) {
    return to.isCompatible(from)
        &&
        // cannot alter from TEXT to STRING because we cannot add statistic to the existing chunks
        !(from == TSDataType.TEXT && to == TSDataType.STRING);
  }

  public void update(long time, boolean value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, int value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, long value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, float value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, double value) {
    update(time);
    updateStats(value);
  }

  public void update(long time, Binary value) {
    update(time);
    updateStats(value);
  }

  public void update(long time) {
    if (time < startTime) {
      startTime = time;
    }
    if (time > endTime) {
      endTime = time;
    }
    count++;
  }

  public void update(long[] time, boolean[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, long[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, float[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, double[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, Binary[] values, int batchSize) {
    update(time, batchSize);
    updateStats(values, batchSize);
  }

  public void update(long[] time, int batchSize) {
    if (time[0] < startTime) {
      startTime = time[0];
    }
    if (time[batchSize - 1] > this.endTime) {
      endTime = time[batchSize - 1];
    }
    count += batchSize;
  }

  public void update(long[] time, int batchSize, int arrayOffset) {
    if (time[arrayOffset] < startTime) {
      startTime = time[arrayOffset];
    }
    if (time[arrayOffset + batchSize - 1] > this.endTime) {
      endTime = time[arrayOffset + batchSize - 1];
    }
    count += batchSize;
  }

  @SuppressWarnings("rawtypes")
  protected abstract void mergeStatisticsValue(Statistics stats);

  public boolean isEmpty() {
    return isEmpty;
  }

  public void setEmpty(boolean empty) {
    isEmpty = empty;
  }

  void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  void updateStats(boolean[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(int[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(long[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(float[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(double[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  void updateStats(Binary[] values, int batchSize) {
    throw new UnsupportedOperationException();
  }

  /**
   * This method with two parameters is only used by {@code unsequence} which
   * updates/inserts/deletes timestamp.
   *
   * @param min min timestamp
   * @param max max timestamp
   * @throws UnsupportedOperationException throw exception when executing this method
   */
  public void updateStats(long min, long max) {
    throw new UnsupportedOperationException();
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public int getCount() {
    return count;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public boolean containedByTimeFilter(Filter timeFilter) {
    return timeFilter == null || timeFilter.containStartEndTime(getStartTime(), getEndTime());
  }

  public boolean hasNullValue(long timeRowCount) {
    return getCount() != timeRowCount;
  }

  @Override
  public String toString() {
    return "startTime: " + startTime + " endTime: " + endTime + " count: " + count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o != null && getClass() == o.getClass();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), count, startTime, endTime);
  }
}
