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

import org.apache.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;
import java.util.List;

public class AlignedChunkMetadata extends AbstractAlignedChunkMetadata {

  public AlignedChunkMetadata(
      IChunkMetadata timeChunkMetadata, List<IChunkMetadata> valueChunkMetadataList) {
    super(timeChunkMetadata, valueChunkMetadataList);
  }

  @Override
  public AbstractAlignedChunkMetadata createNewChunkMetadata(
      IChunkMetadata timeChunkMetadata, List<IChunkMetadata> valueChunkMetadataList) {
    return new AlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return valueChunkMetadataList.size() == 1 && valueChunkMetadataList.get(0) != null
        ? valueChunkMetadataList.get(0).getStatistics()
        : timeChunkMetadata.getStatistics();
  }

  @Override
  public boolean timeAllSelected() {
    for (int index = 0; index < getMeasurementCount(); index++) {
      if (!hasNullValue(index)) {
        // When there is any value page point number that is the same as the time page,
        // it means that all timestamps in time page will be selected.
        return true;
      }
    }
    return false;
  }
}
