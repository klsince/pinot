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
package org.apache.pinot.common.metadata.segment;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.joda.time.Interval;


public class SegmentZKMetadataUtils {
  private SegmentZKMetadataUtils() {
  }

  /**
   * Creates the segment ZK metadata for a new segment.
   */
  public static SegmentZKMetadata createSegmentZKMetadata(String tableNameWithType, SegmentMetadata segmentMetadata,
      String downloadUrl, @Nullable String crypterName, long segmentSizeInBytes) {
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentMetadata.getName());
    updateSegmentZKMetadata(tableNameWithType, segmentZKMetadata, segmentMetadata, downloadUrl, crypterName,
        segmentSizeInBytes, null, true);
    return segmentZKMetadata;
  }

  /**
   * Refreshes the segment ZK metadata for a segment being replaced.
   */
  public static void refreshSegmentZKMetadata(String tableNameWithType, SegmentZKMetadata segmentZKMetadata,
      SegmentMetadata segmentMetadata, String downloadUrl, @Nullable String crypterName, long segmentSizeInBytes) {
    updateSegmentZKMetadata(tableNameWithType, segmentZKMetadata, segmentMetadata, downloadUrl, crypterName,
        segmentSizeInBytes, null, false);
  }

  /**
   * Updates the segment ZK metadata for a committing segment.
   */
  public static void updateCommittingSegmentZKMetadata(String realtimeTableName, SegmentZKMetadata segmentZKMetadata,
      SegmentMetadata segmentMetadata, String downloadUrl, long segmentSizeInBytes, String endOffset) {
    updateSegmentZKMetadata(realtimeTableName, segmentZKMetadata, segmentMetadata, downloadUrl, null,
        segmentSizeInBytes, endOffset, false);
  }

  public static void updateSegmentZKTimeInterval(SegmentZKMetadata segmentZKMetadata,
      DateTimeFieldSpec dateTimeFieldSpec) {
    String startTimeString = segmentZKMetadata.getRawStartTime();
    if (StringUtils.isNotEmpty(startTimeString)) {
      long updatedStartTime = dateTimeFieldSpec.getFormatSpec().fromFormatToMillis(startTimeString);
      segmentZKMetadata.setStartTime(updatedStartTime);
    }

    String endTimeString = segmentZKMetadata.getRawEndTime();
    if (StringUtils.isNotEmpty(endTimeString)) {
      long updatedEndTime = dateTimeFieldSpec.getFormatSpec().fromFormatToMillis(endTimeString);
      segmentZKMetadata.setEndTime(updatedEndTime);
    }
  }

  private static void updateSegmentZKMetadata(String tableNameWithType, SegmentZKMetadata segmentZKMetadata,
      SegmentMetadata segmentMetadata, String downloadUrl, @Nullable String crypterName, long segmentSizeInBytes,
      @Nullable String endOffset, boolean newSegment) {
    String segmentName = segmentZKMetadata.getSegmentName();

    if (endOffset != null) {
      // For committing segment

      segmentZKMetadata.setEndOffset(endOffset);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);

      // For committing segment, use current time as start/end time if total docs is 0
      if (segmentMetadata.getTotalDocs() > 0) {
        Interval timeInterval = segmentMetadata.getTimeInterval();
        Preconditions.checkState(timeInterval != null, "Failed to find time info for table: %s, segment: %s",
            tableNameWithType, segmentName);
        segmentZKMetadata.setStartTime(timeInterval.getStartMillis());
        segmentZKMetadata.setEndTime(timeInterval.getEndMillis());
      } else {
        long now = System.currentTimeMillis();
        segmentZKMetadata.setStartTime(now);
        segmentZKMetadata.setEndTime(now);
      }
      segmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    } else {
      // For uploaded segment

      // Set segment status, start/end offset info for real-time table
      if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
        segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.UPLOADED);

        // For new segment, start/end offset must exist if the segment name follows LLC segment name convention
        if (newSegment && LLCSegmentName.isLLCSegment(segmentMetadata.getName())) {
          Preconditions.checkArgument(
              segmentMetadata.getStartOffset() != null && segmentMetadata.getEndOffset() != null,
              "New uploaded LLC segment must have start/end offset in the segment metadata");
        }

        // NOTE:
        // - If start/end offset is available in the uploaded segment, update them in the segment ZK metadata
        // - If not, keep the existing start/end offset in the segment ZK metadata unchanged
        if (segmentMetadata.getStartOffset() != null) {
          segmentZKMetadata.setStartOffset(segmentMetadata.getStartOffset());
        }
        if (segmentMetadata.getEndOffset() != null) {
          segmentZKMetadata.setEndOffset(segmentMetadata.getEndOffset());
        }
      }

      segmentZKMetadata.setCreationTime(segmentMetadata.getIndexCreationTime());
      if (newSegment) {
        segmentZKMetadata.setPushTime(System.currentTimeMillis());
      } else {
        segmentZKMetadata.setRefreshTime(System.currentTimeMillis());
      }
      Interval timeInterval = segmentMetadata.getTimeInterval();
      if (timeInterval != null) {
        segmentZKMetadata.setStartTime(timeInterval.getStartMillis());
        segmentZKMetadata.setEndTime(timeInterval.getEndMillis());
        segmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
        ColumnMetadata timeColumnMetadata = segmentMetadata.getColumnMetadataFor(segmentMetadata.getTimeColumn());
        if (isValidTimeMetadata(timeColumnMetadata)) {
          segmentZKMetadata.setRawStartTime(timeColumnMetadata.getMinValue().toString());
          segmentZKMetadata.setRawEndTime(timeColumnMetadata.getMaxValue().toString());
        }
      } else {
        segmentZKMetadata.setStartTime(-1);
        segmentZKMetadata.setEndTime(-1);
        segmentZKMetadata.setTimeUnit(null);
      }
    }

    SegmentVersion segmentVersion = segmentMetadata.getVersion();
    segmentZKMetadata.setIndexVersion(segmentVersion != null ? segmentVersion.toString() : null);
    segmentZKMetadata.setTotalDocs(segmentMetadata.getTotalDocs());
    segmentZKMetadata.setCrc(Long.parseLong(segmentMetadata.getCrc()));
    segmentZKMetadata.setDownloadUrl(downloadUrl);
    segmentZKMetadata.setCrypterName(crypterName);
    segmentZKMetadata.setSizeInBytes(segmentSizeInBytes);

    // Set partition metadata
    Map<String, ColumnPartitionMetadata> columnPartitionMap = new HashMap<>();
    for (Map.Entry<String, ColumnMetadata> entry : segmentMetadata.getColumnMetadataMap().entrySet()) {
      ColumnMetadata columnMetadata = entry.getValue();
      PartitionFunction partitionFunction = columnMetadata.getPartitionFunction();
      if (partitionFunction != null) {
        ColumnPartitionMetadata columnPartitionMetadata =
            new ColumnPartitionMetadata(partitionFunction.getName(), partitionFunction.getNumPartitions(),
                columnMetadata.getPartitions(), partitionFunction.getFunctionConfig());
        columnPartitionMap.put(entry.getKey(), columnPartitionMetadata);
      }
    }
    segmentZKMetadata.setPartitionMetadata(
        !columnPartitionMap.isEmpty() ? new SegmentPartitionMetadata(columnPartitionMap) : null);

    // Update custom metadata
    // NOTE: Do not remove existing keys because they can be set by the HTTP header from the segment upload request
    Map<String, String> customMap = segmentZKMetadata.getCustomMap();
    if (customMap == null) {
      customMap = segmentMetadata.getCustomMap();
    } else {
      customMap.putAll(segmentMetadata.getCustomMap());
    }
    segmentZKMetadata.setCustomMap(customMap);
  }

  private static boolean isValidTimeMetadata(ColumnMetadata timeColumnMetadata) {
    return timeColumnMetadata != null && timeColumnMetadata.getMinValue() != null
        && timeColumnMetadata.getMaxValue() != null && !timeColumnMetadata.isMinMaxValueInvalid();
  }
}
