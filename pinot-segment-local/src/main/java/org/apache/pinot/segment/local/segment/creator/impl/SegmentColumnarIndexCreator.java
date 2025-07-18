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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexPlugin;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.SegmentZKPropsConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.*;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment.*;


/**
 * Segment creator which writes data in a columnar form.
 */
// TODO: check resource leaks
public class SegmentColumnarIndexCreator implements SegmentCreator {
  // TODO Refactor class name to match interface name
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);
  // Allow at most 512 characters for the metadata property
  static final int METADATA_PROPERTY_LENGTH_LIMIT = 512;
  private SegmentGeneratorConfig _config;
  private TreeMap<String, ColumnIndexCreationInfo> _indexCreationInfoMap;
  private final Map<String, SegmentDictionaryCreator> _dictionaryCreatorMap = new HashMap<>();
  /**
   * Contains, indexed by column name, the creator associated with each index type.
   *
   * Indexes whose build lifecycle is not DURING_SEGMENT_CREATION are not included here.
   */
  private Map<String, Map<IndexType<?, ?, ?>, IndexCreator>> _creatorsByColAndIndex = new HashMap<>();
  private final Map<String, NullValueVectorCreator> _nullValueVectorCreatorMap = new HashMap<>();
  private String _segmentName;
  private Schema _schema;
  private File _indexDir;
  private int _totalDocs;
  private int _docIdCounter;

  @Override
  public void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      TreeMap<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir,
      @Nullable int[] immutableToMutableIdMap)
      throws Exception {
    _docIdCounter = 0;
    _config = segmentCreationSpec;
    _indexCreationInfoMap = indexCreationInfoMap;

    // Check that the output directory does not exist
    Preconditions.checkState(!outDir.exists(), "Segment output directory: %s already exists", outDir);

    Preconditions.checkState(outDir.mkdirs(), "Failed to create output directory: %s", outDir);
    _indexDir = outDir;

    _schema = schema;
    _totalDocs = segmentIndexCreationInfo.getTotalDocs();
    if (_totalDocs == 0) {
      return;
    }

    Map<String, FieldIndexConfigs> indexConfigs = segmentCreationSpec.getIndexConfigsByColName();

    _creatorsByColAndIndex = Maps.newHashMapWithExpectedSize(indexConfigs.keySet().size());

    for (String columnName : indexConfigs.keySet()) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in the schema", columnName);
      if (fieldSpec.isVirtualColumn()) {
        LOGGER.warn("Ignoring index creation for virtual column {}", columnName);
        continue;
      }

      FieldIndexConfigs originalConfig = indexConfigs.get(columnName);
      ColumnIndexCreationInfo columnIndexCreationInfo = indexCreationInfoMap.get(columnName);
      Preconditions.checkNotNull(columnIndexCreationInfo, "Missing index creation info for column: %s", columnName);
      boolean dictEnabledColumn = createDictionaryForColumn(columnIndexCreationInfo, segmentCreationSpec, fieldSpec);
      if (originalConfig.getConfig(StandardIndexes.inverted()).isEnabled()) {
        Preconditions.checkState(dictEnabledColumn,
            "Cannot create inverted index for raw index column: %s", columnName);
      }

      IndexType<ForwardIndexConfig, ?, ForwardIndexCreator> forwardIdx = StandardIndexes.forward();
      boolean forwardIndexDisabled = !originalConfig.getConfig(forwardIdx).isEnabled();

      //@formatter:off
      IndexCreationContext.Common context = IndexCreationContext.builder()
          .withIndexDir(_indexDir)
          .withDictionary(dictEnabledColumn)
          .withFieldSpec(fieldSpec)
          .withTotalDocs(segmentIndexCreationInfo.getTotalDocs())
          .withColumnIndexCreationInfo(columnIndexCreationInfo)
          .withOptimizedDictionary(_config.isOptimizeDictionary()
              || _config.isOptimizeDictionaryForMetrics() && fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC)
          .onHeap(segmentCreationSpec.isOnHeap())
          .withForwardIndexDisabled(forwardIndexDisabled)
          .withTextCommitOnClose(true)
          .withImmutableToMutableIdMap(immutableToMutableIdMap)
          .withRealtimeConversion(segmentCreationSpec.isRealtimeConversion())
          .withConsumerDir(segmentCreationSpec.getConsumerDir())
          .withTableNameWithType(_config.getTableConfig().getTableName())
          .withContinueOnError(_config.isContinueOnError())
          .build();
      //@formatter:on

      FieldIndexConfigs config = adaptConfig(columnName, originalConfig, columnIndexCreationInfo, segmentCreationSpec);

      if (dictEnabledColumn) {
        // Create dictionary-encoded index
        // Initialize dictionary creator
        // TODO: Dictionary creator holds all unique values on heap. Consider keeping dictionary instead of creator
        //       which uses off-heap memory.

        DictionaryIndexConfig dictConfig = config.getConfig(StandardIndexes.dictionary());
        if (!dictConfig.isEnabled()) {
          LOGGER.info("Creating dictionary index in column {}.{} even when it is disabled in config",
              segmentCreationSpec.getTableName(), columnName);
        }

        // override dictionary type if configured to do so
        if (_config.isOptimizeDictionaryType()) {
          LOGGER.info("Overriding dictionary type for column: {} using var-length dictionary: {}", columnName,
              columnIndexCreationInfo.isUseVarLengthDictionary());
          dictConfig = new DictionaryIndexConfig(dictConfig, columnIndexCreationInfo.isUseVarLengthDictionary());
        }

        SegmentDictionaryCreator creator =
            new DictionaryIndexPlugin().getIndexType().createIndexCreator(context, dictConfig);

        try {
          creator.build(context.getSortedUniqueElementsArray());
        } catch (Exception e) {
          LOGGER.error("Error building dictionary for field: {}, cardinality: {}, number of bytes per entry: {}",
              context.getFieldSpec().getName(), context.getCardinality(), creator.getNumBytesPerEntry());
          throw e;
        }

        _dictionaryCreatorMap.put(columnName, creator);
      }

      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex =
          Maps.newHashMapWithExpectedSize(IndexService.getInstance().getAllIndexes().size());
      for (IndexType<?, ?, ?> index : IndexService.getInstance().getAllIndexes()) {
        if (index.getIndexBuildLifecycle() != IndexType.BuildLifecycle.DURING_SEGMENT_CREATION) {
          continue;
        }
        tryCreateIndexCreator(creatorsByIndex, index, context, config);
      }
      // TODO: Remove this when values stored as ForwardIndex stop depending on TextIndex config
      IndexCreator oldFwdCreator = creatorsByIndex.get(forwardIdx);
      if (oldFwdCreator != null) {
        Object fakeForwardValue = calculateRawValueForTextIndex(dictEnabledColumn, config, fieldSpec);
        if (fakeForwardValue != null) {
          ForwardIndexCreator castedOldFwdCreator = (ForwardIndexCreator) oldFwdCreator;
          SameValueForwardIndexCreator fakeValueFwdCreator =
              new SameValueForwardIndexCreator(fakeForwardValue, castedOldFwdCreator);
          creatorsByIndex.put(forwardIdx, fakeValueFwdCreator);
        }
      }
      _creatorsByColAndIndex.put(columnName, creatorsByIndex);
    }

    // Although NullValueVector is implemented as an index, it needs to be treated in a different way than other indexes
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      String columnName = fieldSpec.getName();
      if (isNullable(fieldSpec)) {
        // Initialize Null value vector map
        LOGGER.info("Column: {} is nullable", columnName);
        _nullValueVectorCreatorMap.put(columnName, new NullValueVectorCreator(_indexDir, columnName));
      } else {
        LOGGER.info("Column: {} is not nullable", columnName);
      }
    }
  }

  private boolean isNullable(FieldSpec fieldSpec) {
    return _schema.isEnableColumnBasedNullHandling() ? fieldSpec.isNullable() : _config.isDefaultNullHandlingEnabled();
  }

  private FieldIndexConfigs adaptConfig(String columnName, FieldIndexConfigs config,
      ColumnIndexCreationInfo columnIndexCreationInfo, SegmentGeneratorConfig segmentCreationSpec) {
    FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder(config);
    // Sorted columns treat the 'forwardIndexDisabled' flag as a no-op
    ForwardIndexConfig fwdConfig = config.getConfig(StandardIndexes.forward());
    if (!fwdConfig.isEnabled() && columnIndexCreationInfo.isSorted()) {
      builder.add(StandardIndexes.forward(),
          new ForwardIndexConfig.Builder(fwdConfig).withLegacyProperties(segmentCreationSpec.getColumnProperties(),
              columnName).build());
    }
    // Initialize inverted index creator; skip creating inverted index if sorted
    if (columnIndexCreationInfo.isSorted()) {
      builder.undeclare(StandardIndexes.inverted());
    }
    return builder.build();
  }

  /**
   * Creates the {@link IndexCreator} in a type safe way.
   *
   * This code needs to be in a specific method instead of inlined in the main loop in order to be able to use the
   * limited generic capabilities of Java.
   */
  private <C extends IndexConfig> void tryCreateIndexCreator(Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex,
      IndexType<C, ?, ?> index, IndexCreationContext.Common context, FieldIndexConfigs fieldIndexConfigs)
      throws Exception {
    C config = fieldIndexConfigs.getConfig(index);
    if (config.isEnabled()) {
      creatorsByIndex.put(index, index.createIndexCreator(context, config));
    }
  }

  /**
   * Returns true if dictionary should be created for a column, false otherwise.
   * Currently there are two sources for this config:
   * <ul>
   *   <li> ColumnIndexCreationInfo (this is currently hard-coded to always return dictionary). </li>
   *   <li> SegmentGeneratorConfig</li>
   * </ul>
   *
   * This method gives preference to the SegmentGeneratorConfig first.
   *
   * @param info Column index creation info
   * @param config Segment generation config
   * @param spec Field spec for the column
   * @return True if dictionary should be created for the column, false otherwise
   */
  private boolean createDictionaryForColumn(ColumnIndexCreationInfo info, SegmentGeneratorConfig config,
      FieldSpec spec) {
    if (spec instanceof ComplexFieldSpec) {
      return false;
    }

    String column = spec.getName();
    FieldIndexConfigs fieldIndexConfigs = config.getIndexConfigsByColName().get(column);
    if (fieldIndexConfigs.getConfig(StandardIndexes.dictionary()).isDisabled()) {
      return false;
    }

    return DictionaryIndexType.ignoreDictionaryOverride(config.isOptimizeDictionary(),
        config.isOptimizeDictionaryForMetrics(), config.getNoDictionarySizeRatioThreshold(),
        config.getNoDictionaryCardinalityRatioThreshold(), spec, fieldIndexConfigs, info.getDistinctValueCount(),
        info.getTotalNumberOfEntries());
  }

  /**
   * @deprecated use {@link ForwardIndexType#getDefaultCompressionType(FieldType)} instead
   */
  @Deprecated
  public static ChunkCompressionType getDefaultCompressionType(FieldType fieldType) {
    return ForwardIndexType.getDefaultCompressionType(fieldType);
  }

  @Override
  public void indexRow(GenericRow row)
      throws IOException {
    for (Map.Entry<String, Map<IndexType<?, ?, ?>, IndexCreator>> byColEntry : _creatorsByColAndIndex.entrySet()) {
      String columnName = byColEntry.getKey();

      Object columnValueToIndex = row.getValue(columnName);
      if (columnValueToIndex == null) {
        throw new RuntimeException("Null value for column:" + columnName);
      }

      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex = byColEntry.getValue();

      FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);
      try {
        if (fieldSpec.isSingleValueField()) {
          indexSingleValueRow(dictionaryCreator, columnValueToIndex, creatorsByIndex);
        } else {
          indexMultiValueRow(dictionaryCreator, (Object[]) columnValueToIndex, creatorsByIndex);
        }
      } catch (JsonParseException jpe) {
        throw new ColumnJsonParserException(columnName, jpe);
      }
    }

    for (Map.Entry<String, NullValueVectorCreator> entry : _nullValueVectorCreatorMap.entrySet()) {
      // If row has null value for given column name, add to null value vector
      if (row.isNullValue(entry.getKey())) {
        entry.getValue().setNull(_docIdCounter);
      }
    }

    _docIdCounter++;
  }

  @Override
  public void indexColumn(String columnName, @Nullable int[] sortedDocIds, IndexSegment segment)
      throws IOException {
    // Iterate over each value in the column
    int numDocs = segment.getSegmentMetadata().getTotalDocs();
    if (numDocs == 0) {
      return;
    }
    try (PinotSegmentColumnReader colReader = new PinotSegmentColumnReader(segment, columnName)) {
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex = _creatorsByColAndIndex.get(columnName);
      NullValueVectorCreator nullVec = _nullValueVectorCreatorMap.get(columnName);
      FieldSpec fieldSpec = _schema.getFieldSpecFor(columnName);
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(columnName);
      if (sortedDocIds != null) {
        int onDiskDocId = 0;
        for (int docId : sortedDocIds) {
          indexColumnValue(colReader, creatorsByIndex, columnName, fieldSpec, dictionaryCreator, docId, onDiskDocId,
              nullVec);
          onDiskDocId++;
        }
      } else {
        for (int docId = 0; docId < numDocs; docId++) {
          indexColumnValue(colReader, creatorsByIndex, columnName, fieldSpec, dictionaryCreator, docId, docId, nullVec);
        }
      }
    }
  }

  private void indexColumnValue(PinotSegmentColumnReader colReader,
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex, String columnName, FieldSpec fieldSpec,
      SegmentDictionaryCreator dictionaryCreator, int sourceDocId, int onDiskDocPos,
      @Nullable NullValueVectorCreator nullVec)
      throws IOException {
    Object columnValueToIndex = colReader.getValue(sourceDocId);
    if (columnValueToIndex == null) {
      throw new RuntimeException("Null value for column:" + columnName);
    }

    if (fieldSpec.isSingleValueField()) {
      indexSingleValueRow(dictionaryCreator, columnValueToIndex, creatorsByIndex);
    } else {
      indexMultiValueRow(dictionaryCreator, (Object[]) columnValueToIndex, creatorsByIndex);
    }

    if (nullVec != null) {
      if (colReader.isNull(sourceDocId)) {
        nullVec.setNull(onDiskDocPos);
      }
    }
  }

  private void indexSingleValueRow(SegmentDictionaryCreator dictionaryCreator, Object value,
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex)
      throws IOException {
    int dictId = dictionaryCreator != null ? dictionaryCreator.indexOfSV(value) : -1;
    for (IndexCreator creator : creatorsByIndex.values()) {
      creator.add(value, dictId);
    }
  }

  private void indexMultiValueRow(SegmentDictionaryCreator dictionaryCreator, Object[] values,
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex)
      throws IOException {
    int[] dictId = dictionaryCreator != null ? dictionaryCreator.indexOfMV(values) : null;
    for (IndexCreator creator : creatorsByIndex.values()) {
      creator.add(values, dictId);
    }
  }

  @Nullable
  private Object calculateRawValueForTextIndex(boolean dictEnabledColumn, FieldIndexConfigs configs,
      FieldSpec fieldSpec) {
    if (dictEnabledColumn) {
      return null;
    }
    TextIndexConfig textConfig = configs.getConfig(StandardIndexes.text());
    if (!textConfig.isEnabled()) {
      return null;
    }

    Object rawValue = textConfig.getRawValueForTextIndex();

    if (rawValue == null) {
      return null;
    } else if (!fieldSpec.isSingleValueField()) {
      if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.STRING) {
        if (!(rawValue instanceof String[])) {
          rawValue = new String[]{String.valueOf(rawValue)};
        }
      } else if (fieldSpec.getDataType().getStoredType() == FieldSpec.DataType.BYTES) {
        if (!(rawValue instanceof String[])) {
          rawValue = new byte[][]{String.valueOf(rawValue).getBytes(StandardCharsets.UTF_8)};
        }
      } else {
        throw new RuntimeException("Text Index is only supported for STRING and BYTES stored type");
      }
    }
    return rawValue;
  }

  @Override
  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  @Override
  public void seal()
      throws ConfigurationException, IOException {
    for (SegmentDictionaryCreator creator : _dictionaryCreatorMap.values()) {
      creator.seal();
    }
    for (NullValueVectorCreator creator : _nullValueVectorCreatorMap.values()) {
      creator.seal();
    }
    for (Map<IndexType<?, ?, ?>, IndexCreator> creatorsByType : _creatorsByColAndIndex.values()) {
      for (IndexCreator creator : creatorsByType.values()) {
        creator.seal();
      }
    }
    writeMetadata();
  }

  private void writeMetadata()
      throws ConfigurationException {
    File metadataFile = new File(_indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromFile(metadataFile);

    properties.setProperty(SEGMENT_CREATOR_VERSION, _config.getCreatorVersion());
    properties.setProperty(SEGMENT_PADDING_CHARACTER, String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR));
    properties.setProperty(SEGMENT_NAME, _segmentName);
    properties.setProperty(TABLE_NAME, _config.getTableName());
    properties.setProperty(DIMENSIONS, _config.getDimensions());
    properties.setProperty(METRICS, _config.getMetrics());
    properties.setProperty(DATETIME_COLUMNS, _config.getDateTimeColumnNames());
    properties.setProperty(COMPLEX_COLUMNS, _config.getComplexColumnNames());
    String timeColumnName = _config.getTimeColumnName();
    properties.setProperty(TIME_COLUMN_NAME, timeColumnName);
    properties.setProperty(SEGMENT_TOTAL_DOCS, String.valueOf(_totalDocs));

    // Write time related metadata (start time, end time, time unit)
    if (timeColumnName != null) {
      ColumnIndexCreationInfo timeColumnIndexCreationInfo = _indexCreationInfoMap.get(timeColumnName);
      if (timeColumnIndexCreationInfo != null) {
        long startTime;
        long endTime;
        TimeUnit timeUnit;

        // Use start/end time in config if defined
        if (_config.getStartTime() != null) {
          startTime = Long.parseLong(_config.getStartTime());
          endTime = Long.parseLong(_config.getEndTime());
          timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
        } else {
          if (_totalDocs > 0) {
            String startTimeStr = timeColumnIndexCreationInfo.getMin().toString();
            String endTimeStr = timeColumnIndexCreationInfo.getMax().toString();

            if (_config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
              // For TimeColumnType.SIMPLE_DATE_FORMAT, convert time value into millis since epoch
              // Use DateTimeFormatter from DateTimeFormatSpec to handle default time zone consistently.
              DateTimeFormatSpec formatSpec = _config.getDateTimeFormatSpec();
              Preconditions.checkNotNull(formatSpec, "DateTimeFormatSpec must exist for SimpleDate");
              DateTimeFormatter dateTimeFormatter = formatSpec.getDateTimeFormatter();
              startTime = dateTimeFormatter.parseMillis(startTimeStr);
              endTime = dateTimeFormatter.parseMillis(endTimeStr);
              timeUnit = TimeUnit.MILLISECONDS;
            } else {
              // by default, time column type is TimeColumnType.EPOCH
              startTime = Long.parseLong(startTimeStr);
              endTime = Long.parseLong(endTimeStr);
              timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
            }
          } else {
            // No records in segment. Use current time as start/end
            long now = System.currentTimeMillis();
            if (_config.getTimeColumnType() == SegmentGeneratorConfig.TimeColumnType.SIMPLE_DATE) {
              startTime = now;
              endTime = now;
              timeUnit = TimeUnit.MILLISECONDS;
            } else {
              timeUnit = Preconditions.checkNotNull(_config.getSegmentTimeUnit());
              startTime = timeUnit.convert(now, TimeUnit.MILLISECONDS);
              endTime = timeUnit.convert(now, TimeUnit.MILLISECONDS);
            }
          }
        }

        if (!_config.isSkipTimeValueCheck()) {
          Interval timeInterval =
              new Interval(timeUnit.toMillis(startTime), timeUnit.toMillis(endTime), DateTimeZone.UTC);
          Preconditions.checkState(TimeUtils.isValidTimeInterval(timeInterval),
              "Invalid segment start/end time: %s (in millis: %s/%s) for time column: %s, must be between: %s",
              timeInterval, timeInterval.getStartMillis(), timeInterval.getEndMillis(), timeColumnName,
              TimeUtils.VALID_TIME_INTERVAL);
        }

        properties.setProperty(SEGMENT_START_TIME, startTime);
        properties.setProperty(SEGMENT_END_TIME, endTime);
        properties.setProperty(TIME_UNIT, timeUnit);
      }
    }

    for (Map.Entry<String, String> entry : _config.getCustomProperties().entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }

    for (Map.Entry<String, ColumnIndexCreationInfo> entry : _indexCreationInfoMap.entrySet()) {
      String column = entry.getKey();
      ColumnIndexCreationInfo columnIndexCreationInfo = entry.getValue();
      SegmentDictionaryCreator dictionaryCreator = _dictionaryCreatorMap.get(column);
      int dictionaryElementSize = (dictionaryCreator != null) ? dictionaryCreator.getNumBytesPerEntry() : 0;
      addColumnMetadataInfo(properties, column, columnIndexCreationInfo, _totalDocs, _schema.getFieldSpecFor(column),
          dictionaryCreator != null, dictionaryElementSize);
    }

    SegmentZKPropsConfig segmentZKPropsConfig = _config.getSegmentZKPropsConfig();
    if (segmentZKPropsConfig != null) {
      properties.setProperty(Realtime.START_OFFSET, segmentZKPropsConfig.getStartOffset());
      properties.setProperty(Realtime.END_OFFSET, segmentZKPropsConfig.getEndOffset());
    }

    CommonsConfigurationUtils.saveToFile(properties, metadataFile);
  }

  public static void addColumnMetadataInfo(PropertiesConfiguration properties, String column,
      ColumnIndexCreationInfo columnIndexCreationInfo, int totalDocs, FieldSpec fieldSpec, boolean hasDictionary,
      int dictionaryElementSize) {
    int cardinality = columnIndexCreationInfo.getDistinctValueCount();
    properties.setProperty(getKeyFor(column, CARDINALITY), String.valueOf(cardinality));
    properties.setProperty(getKeyFor(column, TOTAL_DOCS), String.valueOf(totalDocs));
    DataType dataType = fieldSpec.getDataType();
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(dataType));
    properties.setProperty(getKeyFor(column, BITS_PER_ELEMENT),
        String.valueOf(PinotDataBitSet.getNumBitsPerValue(cardinality - 1)));
    properties.setProperty(getKeyFor(column, DICTIONARY_ELEMENT_SIZE), String.valueOf(dictionaryElementSize));
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    properties.setProperty(getKeyFor(column, IS_SORTED), String.valueOf(columnIndexCreationInfo.isSorted()));
    properties.setProperty(getKeyFor(column, HAS_DICTIONARY), String.valueOf(hasDictionary));
    properties.setProperty(getKeyFor(column, IS_SINGLE_VALUED), String.valueOf(fieldSpec.isSingleValueField()));
    properties.setProperty(getKeyFor(column, MAX_MULTI_VALUE_ELEMENTS),
        String.valueOf(columnIndexCreationInfo.getMaxNumberOfMultiValueElements()));
    properties.setProperty(getKeyFor(column, TOTAL_NUMBER_OF_ENTRIES),
        String.valueOf(columnIndexCreationInfo.getTotalNumberOfEntries()));
    properties.setProperty(getKeyFor(column, IS_AUTO_GENERATED),
        String.valueOf(columnIndexCreationInfo.isAutoGenerated()));
    if (dataType.equals(DataType.STRING) || dataType.equals(DataType.BYTES) || dataType.equals(DataType.JSON)) {
      properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH), fieldSpec.getEffectiveMaxLength());
      // TODO let's revisit writing effective maxLengthStrategy into metadata, as changing it right now may affect
      //  segment's CRC value
      FieldSpec.MaxLengthExceedStrategy maxLengthStrategy = fieldSpec.getMaxLengthExceedStrategy();
      if (maxLengthStrategy != null) {
        properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH_EXCEED_STRATEGY),
            fieldSpec.getMaxLengthExceedStrategy());
      }
    }

    PartitionFunction partitionFunction = columnIndexCreationInfo.getPartitionFunction();
    if (partitionFunction != null) {
      properties.setProperty(getKeyFor(column, PARTITION_FUNCTION), partitionFunction.getName());
      properties.setProperty(getKeyFor(column, NUM_PARTITIONS), columnIndexCreationInfo.getNumPartitions());
      properties.setProperty(getKeyFor(column, PARTITION_VALUES), columnIndexCreationInfo.getPartitions());
      if (columnIndexCreationInfo.getPartitionFunctionConfig() != null) {
        for (Map.Entry<String, String> entry : columnIndexCreationInfo.getPartitionFunctionConfig().entrySet()) {
          properties.setProperty(getKeyFor(column, String.format("%s.%s", PARTITION_FUNCTION_CONFIG, entry.getKey())),
              entry.getValue());
        }
      }
    }

    // datetime field
    if (fieldSpec.getFieldType() == FieldType.DATE_TIME) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, DATETIME_FORMAT), dateTimeFieldSpec.getFormat());
      properties.setProperty(getKeyFor(column, DATETIME_GRANULARITY), dateTimeFieldSpec.getGranularity());
    }

    // complex field
    if (fieldSpec.getFieldType() == FieldType.COMPLEX) {
      ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, COMPLEX_CHILD_FIELD_NAMES),
          new ArrayList<>(complexFieldSpec.getChildFieldSpecs().keySet()));
      for (Map.Entry<String, FieldSpec> entry : complexFieldSpec.getChildFieldSpecs().entrySet()) {
        addFieldSpec(properties, ComplexFieldSpec.getFullChildName(column, entry.getKey()), entry.getValue());
      }
    }

    // NOTE: Min/max could be null for real-time aggregate metrics.
    if ((fieldSpec.getFieldType() != FieldType.COMPLEX) && (totalDocs > 0)) {
      Object min = columnIndexCreationInfo.getMin();
      Object max = columnIndexCreationInfo.getMax();
      if (min != null && max != null) {
        addColumnMinMaxValueInfo(properties, column, min, max, dataType.getStoredType());
      }
    }

    String defaultNullValue = columnIndexCreationInfo.getDefaultNullValue().toString();
    if (dataType.getStoredType() == DataType.STRING) {
      // NOTE: Do not limit length of default null value because we need exact value to determine whether the default
      //       null value changes
      defaultNullValue = CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(defaultNullValue);
    }
    if (defaultNullValue != null) {
      properties.setProperty(getKeyFor(column, DEFAULT_NULL_VALUE), defaultNullValue);
    }
  }

  /**
   * In order to persist complex field metadata, we need to recursively add child field specs
   * So, each complex field spec will have a property for its child field names and each child field will have its
   * own properties of the detailed field spec.
   * E.g. a COMPLEX type `intMap` of Map<String, Integer> has 2 child fields:
   *   - key in STRING type and value in INT type.
   *   Then we will have the following properties to define a COMPLEX field:
   *     column.intMap.childFieldNames = [key, value]
   *     column.intMap$$key.columnType = DIMENSION
   *     column.intMap$$key.dataType = STRING
   *     column.intMap$$key.isSingleValued = true
   *     column.intMap$$value.columnType = DIMENSION
   *     column.intMap$$value.dataType = INT
   *     column.intMap$$value.isSingleValued = true
   */
  public static void addFieldSpec(PropertiesConfiguration properties, String column, FieldSpec fieldSpec) {
    properties.setProperty(getKeyFor(column, COLUMN_TYPE), String.valueOf(fieldSpec.getFieldType()));
    if (!column.equals(fieldSpec.getName())) {
      properties.setProperty(getKeyFor(column, COLUMN_NAME), String.valueOf(fieldSpec.getName()));
    }
    DataType dataType = fieldSpec.getDataType();
    properties.setProperty(getKeyFor(column, DATA_TYPE), String.valueOf(dataType));
    properties.setProperty(getKeyFor(column, IS_SINGLE_VALUED), String.valueOf(fieldSpec.isSingleValueField()));
    if (dataType.equals(DataType.STRING) || dataType.equals(DataType.BYTES) || dataType.equals(DataType.JSON)) {
      properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH), fieldSpec.getEffectiveMaxLength());
      FieldSpec.MaxLengthExceedStrategy maxLengthExceedStrategy = fieldSpec.getEffectiveMaxLengthExceedStrategy();
      if (maxLengthExceedStrategy != null) {
        properties.setProperty(getKeyFor(column, SCHEMA_MAX_LENGTH_EXCEED_STRATEGY), maxLengthExceedStrategy);
      }
    }

    // datetime field
    if (fieldSpec.getFieldType() == FieldType.DATE_TIME) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, DATETIME_FORMAT), dateTimeFieldSpec.getFormat());
      properties.setProperty(getKeyFor(column, DATETIME_GRANULARITY), dateTimeFieldSpec.getGranularity());
    }

    // complex field
    if (fieldSpec.getFieldType() == FieldType.COMPLEX) {
      ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) fieldSpec;
      properties.setProperty(getKeyFor(column, COMPLEX_CHILD_FIELD_NAMES),
          new ArrayList<>(complexFieldSpec.getChildFieldSpecs().keySet()));
      for (Map.Entry<String, FieldSpec> entry : complexFieldSpec.getChildFieldSpecs().entrySet()) {
        addFieldSpec(properties, ComplexFieldSpec.getFullChildName(column, entry.getKey()), entry.getValue());
      }
    }
  }

  public static void addColumnMinMaxValueInfo(PropertiesConfiguration properties, String column,
      @Nullable Object minValue, @Nullable Object maxValue, DataType storedType) {
    String validMinValue = minValue != null ? getValidPropertyValue(minValue.toString(), storedType) : null;
    if (validMinValue != null) {
      properties.setProperty(getKeyFor(column, MIN_VALUE), validMinValue);
    }
    String validMaxValue = maxValue != null ? getValidPropertyValue(maxValue.toString(), storedType) : null;
    if (validMaxValue != null) {
      properties.setProperty(getKeyFor(column, MAX_VALUE), validMaxValue);
    }
    if (validMinValue == null && validMaxValue == null) {
      properties.setProperty(getKeyFor(column, MIN_MAX_VALUE_INVALID), true);
    }
  }

  /**
   * Helper method to get the valid value for setting min/max. Returns {@code null} if the value is too long (longer
   * than 512 characters), or is not supported in {@link PropertiesConfiguration}, e.g. contains character with
   * surrogate.
   */
  @Nullable
  private static String getValidPropertyValue(String value, DataType storedType) {
    if (value.length() > METADATA_PROPERTY_LENGTH_LIMIT) {
      return null;
    }
    return storedType == DataType.STRING ? CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(value)
        : value;
  }

  public static void removeColumnMetadataInfo(PropertiesConfiguration properties, String column) {
    properties.subset(COLUMN_PROPS_KEY_PREFIX + column).clear();
  }

  @Override
  public void close()
      throws IOException {
    List<Closeable> creators =
        _creatorsByColAndIndex.values().stream().flatMap(map -> map.values().stream()).collect(Collectors.toList());
    creators.addAll(_nullValueVectorCreatorMap.values());
    creators.addAll(_dictionaryCreatorMap.values());
    FileUtils.close(creators);
  }
}
