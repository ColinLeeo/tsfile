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
package org.apache.tsfile.read.query.timegenerator;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.impl.BinaryExpression;
import org.apache.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsFileGeneratorForTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class TimeGeneratorTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader fileReader;
  private MetadataQuerierByFileImpl metadataQuerierByFile;
  private IChunkLoader chunkLoader;

  @Before
  public void before() throws IOException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    TsFileGeneratorForTest.generateFile(1000, 10 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(FILE_PATH);
    metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
    chunkLoader = new CachedChunkLoaderImpl(fileReader);
  }

  @After
  public void after() throws IOException {
    fileReader.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void testTimeGenerator() throws IOException {
    long startTimestamp = 1480562618000L;
    Filter filter = TimeFilterApi.lt(1480562618100L);
    Filter filter2 =
        ValueFilterApi.gt(
            DEFAULT_MEASUREMENT_INDEX,
            new Binary("dog", TSFileConfig.STRING_CHARSET),
            TSDataType.TEXT);
    Filter filter3 =
        FilterFactory.and(TimeFilterApi.gtEq(1480562618000L), TimeFilterApi.ltEq(1480562618100L));

    IExpression IExpression =
        BinaryExpression.or(
            BinaryExpression.and(
                new SingleSeriesExpression(new Path("d1", "s1", true), filter),
                new SingleSeriesExpression(new Path("d1", "s4", true), filter2)),
            new SingleSeriesExpression(new Path("d1", "s1", true), filter3));

    TsFileTimeGenerator timestampGenerator =
        new TsFileTimeGenerator(IExpression, chunkLoader, metadataQuerierByFile);
    while (timestampGenerator.hasNext()) {
      // System.out.println(timestampGenerator.next());
      Assert.assertEquals(startTimestamp, timestampGenerator.next());
      startTimestamp += 1;
    }
    Assert.assertEquals(1480562618101L, startTimestamp);
  }
}
