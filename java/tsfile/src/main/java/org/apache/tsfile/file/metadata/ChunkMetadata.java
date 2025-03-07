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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.tsfile.utils.Preconditions.checkArgument;

/** Metadata of one chunk. */
public class ChunkMetadata implements IChunkMetadata {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ChunkMetadata.class)
          + RamUsageEstimator.shallowSizeOfInstance(TSDataType.class)
          + RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);

  private String measurementUid;

  /**
   * Byte offset of the corresponding data in the file Notice: include the chunk header and marker.
   */
  private long offsetOfChunkHeader;

  private TSDataType tsDataType;

  private TSDataType newTsDataType;

  // the following two are not serialized and only used during write
  private TSEncoding encoding;
  private CompressionType compressionType;

  /**
   * version is used to define the order of operations(insertion, deletion, update). version is set
   * according to its belonging ChunkGroup only when being queried, so it is not persisted.
   */
  private long version;

  /** A list of deleted intervals. */
  private ArrayList<TimeRange> deleteIntervalList;

  private boolean modified;

  /** ChunkLoader of metadata, used to create ChunkReaderWrap. */
  private IChunkLoader chunkLoader;

  private Statistics<? extends Serializable> statistics;

  private static final int CHUNK_METADATA_FIXED_RAM_SIZE = 93;

  // used for SeriesReader to indicate whether it is a seq/unseq timeseries metadata
  private boolean isSeq = true;
  private boolean isClosed;

  // 0x80 for time chunk, 0x40 for value chunk, 0x00 for common chunk
  private byte mask;

  public ChunkMetadata() {}

  /**
   * constructor of ChunkMetaData.
   *
   * @param measurementUid measurement id
   * @param tsDataType time series data type
   * @param fileOffset file offset
   * @param statistics value statistics
   */
  public ChunkMetadata(
      String measurementUid,
      TSDataType tsDataType,
      TSEncoding encoding,
      CompressionType compressionType,
      long fileOffset,
      Statistics<? extends Serializable> statistics) {
    this.measurementUid = measurementUid;
    this.tsDataType = tsDataType;
    this.offsetOfChunkHeader = fileOffset;
    this.statistics = statistics;
    this.encoding = encoding;
    this.compressionType = compressionType;
  }

  // won't clone deleteIntervalList & modified
  public ChunkMetadata(ChunkMetadata other) {
    this.measurementUid = other.measurementUid;
    this.offsetOfChunkHeader = other.offsetOfChunkHeader;
    this.tsDataType = other.tsDataType;
    this.version = other.version;
    this.statistics = other.statistics;
    this.isSeq = other.isSeq;
    this.isClosed = other.isClosed;
    this.mask = other.mask;
    this.encoding = other.encoding;
    this.compressionType = other.compressionType;
  }

  @Override
  public String toString() {
    return String.format(
        "measurementId: %s, datatype: %s, version: %d, " + "Statistics: %s, deleteIntervalList: %s",
        measurementUid, tsDataType, version, statistics, deleteIntervalList);
  }

  public long getNumOfPoints() {
    return statistics.getCount();
  }

  /**
   * get offset of chunk header.
   *
   * @return Byte offset of header of this chunk (includes the marker)
   */
  @Override
  public long getOffsetOfChunkHeader() {
    return offsetOfChunkHeader;
  }

  public String getMeasurementUid() {
    return measurementUid;
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return statistics;
  }

  public long getStartTime() {
    return statistics.getStartTime();
  }

  public long getEndTime() {
    return statistics.getEndTime();
  }

  public TSDataType getDataType() {
    return tsDataType;
  }

  public void setTsDataType(TSDataType tsDataType) {
    this.tsDataType = tsDataType;
  }

  public void setStatistics(Statistics statistics) {
    this.statistics = statistics;
  }

  /**
   * serialize to outputStream.
   *
   * @param outputStream outputStream
   * @return length
   * @throws IOException IOException
   */
  public int serializeTo(OutputStream outputStream, boolean serializeStatistic) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(offsetOfChunkHeader, outputStream);
    if (serializeStatistic) {
      byteLen += statistics.serialize(outputStream);
    }
    return byteLen;
  }

  public int serializedSize(boolean includeStatistics) {
    int cnt = Long.BYTES; // offsetOfChunkHeader
    if (includeStatistics) {
      cnt += statistics.getSerializedSize();
    }
    return cnt;
  }

  /**
   * deserialize from ByteBuffer.
   *
   * @param buffer ByteBuffer
   * @return ChunkMetaData object
   */
  public static ChunkMetadata deserializeFrom(
      ByteBuffer buffer, TimeseriesMetadata timeseriesMetadata) {
    ChunkMetadata chunkMetaData = new ChunkMetadata();

    chunkMetaData.measurementUid = timeseriesMetadata.getMeasurementId();
    chunkMetaData.tsDataType = timeseriesMetadata.getTsDataType();
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    // if the TimeSeriesMetadataType is not 0, it means it has more than one chunk
    // and each chunk's metadata has its own statistics
    if ((timeseriesMetadata.getTimeSeriesMetadataType() & 0x3F) != 0) {
      chunkMetaData.statistics = Statistics.deserialize(buffer, chunkMetaData.tsDataType);
    } else {
      // if the TimeSeriesMetadataType is 0, it means it has only one chunk
      // and that chunk's metadata has no statistic
      chunkMetaData.statistics = timeseriesMetadata.getStatistics();
    }
    return chunkMetaData;
  }

  public static ChunkMetadata deserializeFrom(
      InputStream inputStream, TimeseriesMetadata timeseriesMetadata) throws IOException {
    ChunkMetadata chunkMetaData = new ChunkMetadata();

    chunkMetaData.measurementUid = timeseriesMetadata.getMeasurementId();
    chunkMetaData.tsDataType = timeseriesMetadata.getTsDataType();
    chunkMetaData.offsetOfChunkHeader = ReadWriteIOUtils.readLong(inputStream);
    // if the TimeSeriesMetadataType is not 0, it means it has more than one chunk
    // and each chunk's metadata has its own statistics
    if ((timeseriesMetadata.getTimeSeriesMetadataType() & 0x3F) != 0) {
      chunkMetaData.statistics = Statistics.deserialize(inputStream, chunkMetaData.tsDataType);
    } else {
      // if the TimeSeriesMetadataType is 0, it means it has only one chunk
      // and that chunk's metadata has no statistic
      chunkMetaData.statistics = timeseriesMetadata.getStatistics();
    }
    return chunkMetaData;
  }

  public static ChunkMetadata deserializeFrom(ByteBuffer buffer, TSDataType dataType) {
    ChunkMetadata chunkMetadata = new ChunkMetadata();
    chunkMetadata.tsDataType = dataType;
    chunkMetadata.offsetOfChunkHeader = ReadWriteIOUtils.readLong(buffer);
    chunkMetadata.statistics = Statistics.deserialize(buffer, dataType);
    return chunkMetadata;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public void setVersion(long version) {
    this.version = version;
  }

  @Override
  public TSDataType getNewType() {
    return newTsDataType;
  }

  @Override
  public void setNewType(TSDataType type) {
    this.newTsDataType = type;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public void insertIntoSortedDeletions(TimeRange timeRange) {
    ArrayList<TimeRange> resultInterval = new ArrayList<>();
    long startTime = timeRange.getMin();
    long endTime = timeRange.getMax();
    if (deleteIntervalList != null) {
      if (deleteIntervalList.get(deleteIntervalList.size() - 1).getMax() < timeRange.getMin()) {
        deleteIntervalList.add(timeRange);
        return;
      }
      for (int i = 0; i < deleteIntervalList.size(); i++) {
        TimeRange interval = deleteIntervalList.get(i);
        if (interval.getMax() < startTime) {
          resultInterval.add(interval);
        } else if (interval.getMin() > endTime) {
          // remaining TimeRanges are in order, add all and return
          resultInterval.add(new TimeRange(startTime, endTime));
          resultInterval.addAll(deleteIntervalList.subList(i, deleteIntervalList.size()));
          deleteIntervalList = resultInterval;
          return;
        } else if (interval.getMax() >= startTime || interval.getMin() <= endTime) {
          startTime = Math.min(interval.getMin(), startTime);
          endTime = Math.max(interval.getMax(), endTime);
        }
      }
    }

    resultInterval.add(new TimeRange(startTime, endTime));
    deleteIntervalList = resultInterval;
  }

  public IChunkLoader getChunkLoader() {
    return chunkLoader;
  }

  @Override
  public boolean needSetChunkLoader() {
    return chunkLoader == null;
  }

  public void setChunkLoader(IChunkLoader chunkLoader) {
    this.chunkLoader = chunkLoader;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @Override
  public void setModified(boolean modified) {
    this.modified = modified;
  }

  public static long calculateRamSize(String measurementId, TSDataType dataType) {
    return CHUNK_METADATA_FIXED_RAM_SIZE
        + RamUsageEstimator.sizeOf(measurementId)
        + Statistics.getSizeByType(dataType);
  }

  public void mergeChunkMetadata(ChunkMetadata chunkMetadata) {
    Statistics<? extends Serializable> chunkMetadataStatistics = chunkMetadata.getStatistics();
    this.statistics.mergeStatistics(chunkMetadataStatistics);
  }

  // it's only used for query cache, measurementUid is from TimeSeriesMetadata
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE + (statistics == null ? 0L : statistics.getRetainedSizeInBytes());
  }

  @Override
  public void setSeq(boolean seq) {
    isSeq = seq;
  }

  @Override
  public boolean isSeq() {
    return isSeq;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public void setClosed(boolean closed) {
    isClosed = closed;
  }

  @Override
  public byte getMask() {
    return mask;
  }

  public void setMask(byte mask) {
    this.mask = mask;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChunkMetadata that = (ChunkMetadata) o;
    return offsetOfChunkHeader == that.offsetOfChunkHeader
        && version == that.version
        && isSeq == that.isSeq
        && isClosed == that.isClosed
        && mask == that.mask
        && Objects.equals(measurementUid, that.measurementUid)
        && tsDataType == that.tsDataType
        && Objects.equals(statistics, that.statistics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        measurementUid,
        offsetOfChunkHeader,
        tsDataType,
        version,
        statistics,
        isSeq,
        isClosed,
        mask);
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return getStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    checkArgument(
        measurementIndex == 0,
        "Non-aligned chunk only has one measurement, but measurementIndex is " + measurementIndex);
    return Optional.ofNullable(statistics);
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    return false;
  }

  // TODO: replaces fields in this class with MeasurementSchema
  public MeasurementSchema toMeasurementSchema() {
    return new MeasurementSchema(measurementUid, tsDataType, encoding, compressionType);
  }
}
