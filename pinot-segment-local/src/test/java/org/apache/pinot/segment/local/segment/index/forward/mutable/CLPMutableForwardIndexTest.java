/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.forward.mutable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.CLPMutableForwardIndexV2;
import org.apache.pinot.segment.local.segment.creator.impl.stats.CLPStatsProvider;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CLPMutableForwardIndexTest implements PinotBuffersAfterClassCheckRule {
  private final List<String> _logLines = new ArrayList<>() {{
    add("2023/10/26 00:03:10.168 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32c_DEFAULT)] "
        + "Event pinot::DEFAULT::4a02a32c_DEFAULT : Refreshed 35 property LiveInstance took 5 ms. Selective:"
        + " true");
    add("2023/10/26 00:03:10.169 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32d_DEFAULT)] "
        + "Event pinot::DEFAULT::4a02a32d_DEFAULT : Refreshed 81 property LiveInstance took 4 ms. Selective:"
        + " true");
    add("2023/10/27 16:35:10.470 INFO [ControllerResponseFilter] [grizzly-http-server-2] Handled request from 0.0"
        + ".0.0 GET https://0.0.0.0:8443/health?checkType=liveness, content-type null status code 200 OK");
    add("2023/10/27 16:35:10.607 INFO [ControllerResponseFilter] [grizzly-http-server-6] Handled request from 0.0"
        + ".0.0 GET https://pinot-pinot-broker-headless.managed.svc.cluster.local:8093/tables, content-type "
        + "application/json status code 200 OK");
    add("null");
  }};
  private final int _rows = 3;

  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(VarByteSVMutableForwardIndexTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  /**
   * Test using CLPMutableForwardIndex
   */
  @Test
  public void testString()
      throws IOException {
    // use arbitrary cardinality and avg string length
    // we will test with complete randomness
    int initialCapacity = 5;
    try (CLPMutableForwardIndex readerWriter = new CLPMutableForwardIndex("col1", FieldSpec.DataType.STRING,
        _memoryManager, initialCapacity)) {
      ingestData(readerWriter);
      validateStats(readerWriter.getCLPStats());
    }
  }

  /**
   * Same as testString() except it is using CLPMutableForwardIndexV2
   */
  @Test
  public void testStringV2()
      throws IOException {
    try (CLPMutableForwardIndexV2 readerWriter = new CLPMutableForwardIndexV2("col1", _memoryManager)) {
      readerWriter.forceClpEncoding();
      ingestData(readerWriter);
      validateStats(readerWriter.getCLPStats());
    }
  }

  private void ingestData(MutableForwardIndex readerWriter) {
    for (int i = 0; i < _rows; i++) {
      readerWriter.setString(i, _logLines.get(i));
    }

    for (int i = 0; i < _rows; i++) {
      Assert.assertEquals(readerWriter.getString(i), _logLines.get(i));
    }
  }

  private void validateStats(CLPStatsProvider.CLPStats mutableIndexStats) {
    // Verify clp stats
    StringColumnPreIndexStatsCollector.CLPStatsCollector statsCollector =
        new StringColumnPreIndexStatsCollector.CLPStatsCollector();
    for (int i = 0; i < _rows; i++) {
      statsCollector.collect(_logLines.get(i));
    }
    statsCollector.seal();
    CLPStatsProvider.CLPStats stats = statsCollector.getCLPStats();

    Assert.assertEquals(stats.getTotalNumberOfDictVars(), mutableIndexStats.getTotalNumberOfDictVars());
    Assert.assertEquals(stats.getMaxNumberOfEncodedVars(), mutableIndexStats.getMaxNumberOfEncodedVars());
    Assert.assertEquals(stats.getSortedDictVarValues(), mutableIndexStats.getSortedDictVarValues());
    Assert.assertEquals(stats.getTotalNumberOfEncodedVars(), mutableIndexStats.getTotalNumberOfEncodedVars());
    Assert.assertEquals(stats.getSortedLogTypeValues(), mutableIndexStats.getSortedLogTypeValues());
    Assert.assertEquals(stats.getSortedDictVarValues(), mutableIndexStats.getSortedDictVarValues());
  }
}
