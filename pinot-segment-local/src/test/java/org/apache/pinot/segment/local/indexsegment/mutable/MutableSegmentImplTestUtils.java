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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.data.Schema;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MutableSegmentImplTestUtils {
  private MutableSegmentImplTestUtils() {
  }

  private static final String TABLE_NAME_WITH_TYPE = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testSegment__0__0__155555";
  private static final String STREAM_NAME = "testStream";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MutableSegmentImplTestUtils");

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema) {
    return createMutableSegmentImpl(schema, Set.of(), Set.of(), Set.of(), false);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns,
      @Nullable List<AggregationConfig> preAggregationConfigs) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        Map.of(), false, preAggregationConfigs, false, null, null, null, null);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns, boolean aggregateMetrics) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        aggregateMetrics, false);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns, boolean aggregateMetrics,
      boolean nullHandlingEnabled) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        aggregateMetrics, nullHandlingEnabled, null, null, null);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, boolean nullHandlingEnabled,
      @Nullable String timeColumnName, @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      @Nullable PartitionDedupMetadataManager partitionDedupMetadataManager) {
    return createMutableSegmentImpl(schema, Set.of(), Set.of(), Set.of(), false, nullHandlingEnabled, timeColumnName,
        partitionUpsertMetadataManager, partitionDedupMetadataManager);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns, boolean aggregateMetrics,
      boolean nullHandlingEnabled, @Nullable String timeColumnName,
      @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      @Nullable PartitionDedupMetadataManager partitionDedupMetadataManager) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        Map.of(), aggregateMetrics, null, nullHandlingEnabled, timeColumnName, partitionUpsertMetadataManager,
        partitionDedupMetadataManager, null);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns,
      Map<String, JsonIndexConfig> jsonIndexConfigs, @Nullable ServerMetrics serverMetrics) {
    return createMutableSegmentImpl(schema, noDictionaryColumns, varLengthDictionaryColumns, invertedIndexColumns,
        jsonIndexConfigs, false, null, true, null, null, null, serverMetrics);
  }

  public static MutableSegmentImpl createMutableSegmentImpl(Schema schema, Set<String> noDictionaryColumns,
      Set<String> varLengthDictionaryColumns, Set<String> invertedIndexColumns,
      Map<String, JsonIndexConfig> jsonIndexConfigs, boolean aggregateMetrics,
      @Nullable List<AggregationConfig> aggregationConfigs, boolean nullHandlingEnabled,
      @Nullable String timeColumnName, @Nullable PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      @Nullable PartitionDedupMetadataManager partitionDedupMetadataManager, @Nullable ServerMetrics serverMetrics) {

    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);

    DictionaryIndexConfig varLengthDictConf = new DictionaryIndexConfig(false, true);

    RealtimeSegmentConfig.Builder segmentConfBuilder = new RealtimeSegmentConfig.Builder()
        .setTableNameWithType(TABLE_NAME_WITH_TYPE)
        .setSegmentName(SEGMENT_NAME)
        .setStreamName(STREAM_NAME)
        .setSchema(schema)
        .setTimeColumnName(timeColumnName)
        .setCapacity(100000)
        .setAvgNumMultiValues(2)
        .setIndex(noDictionaryColumns, StandardIndexes.dictionary(), DictionaryIndexConfig.DISABLED)
        .setIndex(varLengthDictionaryColumns, StandardIndexes.dictionary(), varLengthDictConf)
        .setIndex(invertedIndexColumns, StandardIndexes.inverted(), IndexConfig.ENABLED)
        .setSegmentZKMetadata(new SegmentZKMetadata(SEGMENT_NAME))
        .setMemoryManager(new DirectMemoryManager(SEGMENT_NAME))
        .setStatsHistory(statsHistory)
        .setAggregateMetrics(aggregateMetrics)
        .setIngestionAggregationConfigs(aggregationConfigs)
        .setDefaultNullHandlingEnabled(nullHandlingEnabled)
        .setPartitionUpsertMetadataManager(partitionUpsertMetadataManager)
        .setPartitionDedupMetadataManager(partitionDedupMetadataManager)
        .setConsumerDir(TEMP_DIR.getAbsolutePath() + "/" + UUID.randomUUID() + "/consumerDir");

    for (Map.Entry<String, JsonIndexConfig> entry : jsonIndexConfigs.entrySet()) {
      segmentConfBuilder.setIndex(entry.getKey(), StandardIndexes.json(), entry.getValue());
    }

    RealtimeSegmentConfig realtimeSegmentConfig = segmentConfBuilder.build();
    return new MutableSegmentImpl(realtimeSegmentConfig, serverMetrics);
  }
}
