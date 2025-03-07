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
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.controller.IChunkLoader;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface IChunkMetadata extends IMetadata {

  boolean isModified();

  void setModified(boolean modified);

  boolean isSeq();

  void setSeq(boolean seq);

  long getVersion();

  void setVersion(long version);

  long getOffsetOfChunkHeader();

  long getStartTime();

  long getEndTime();

  IChunkLoader getChunkLoader();

  boolean needSetChunkLoader();

  void setChunkLoader(IChunkLoader chunkLoader);

  void setClosed(boolean closed);

  TSDataType getDataType();

  TSDataType getNewType();

  void setNewType(TSDataType newType);

  String getMeasurementUid();

  void insertIntoSortedDeletions(TimeRange timeRange);

  List<TimeRange> getDeleteIntervalList();

  int serializeTo(OutputStream outputStream, boolean serializeStatistic) throws IOException;

  byte getMask();
}
