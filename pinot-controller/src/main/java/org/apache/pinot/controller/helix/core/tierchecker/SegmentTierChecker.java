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
package org.apache.pinot.controller.helix.core.tierchecker;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.tier.Tier;
import org.apache.pinot.common.tier.TierSegmentSelector;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to calculate the target tier the segment belongs to so that servers can put the segment on to the
 * right storage tier when loading the segment.
 */
public class SegmentTierChecker extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentTierChecker.class);

  public SegmentTierChecker(PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, ControllerConf config, ControllerMetrics controllerMetrics) {
    super(SegmentTierChecker.class.getSimpleName(), config.getSegmentTierCheckerFrequencyInSeconds(),
        config.getSegmentTierCheckerInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
  }

  @Override
  protected void processTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: {}", tableNameWithType);
    if (CollectionUtils.isEmpty(tableConfig.getTierConfigsList())) {
      LOGGER.info("No extra tiers to consider for segments of table: {}", tableNameWithType);
      return;
    }
    LOGGER.info("Checking and updating tiers for segments of table: {}", tableNameWithType);
    List<Tier> sortedTiers = TierConfigUtils
        .getSortedTiers(tableConfig.getTierConfigsList(), _pinotHelixResourceManager.getHelixZkManager());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Sorted tiers: {} configured for table: {}", sortedTiers, tableNameWithType);
    }
    for (String segmentName : _pinotHelixResourceManager.getSegmentsFor(tableNameWithType, true)) {
      updateSegmentTier(tableNameWithType, segmentName, sortedTiers);
    }
  }

  private void updateSegmentTier(String tableNameWithType, String segmentName, List<Tier> sortedTiers) {
    Tier targetTier = null;
    for (Tier tier : sortedTiers) {
      TierSegmentSelector tierSegmentSelector = tier.getSegmentSelector();
      boolean segmentSelected = tierSegmentSelector.selectSegment(tableNameWithType, segmentName);
      if (segmentSelected) {
        targetTier = tier;
        break;
      }
    }
    if (targetTier == null) {
      LOGGER.debug("No target tier for segment: {} of table: {}", segmentName, tableNameWithType);
      return;
    }
    ZNRecord segmentMetadataZNRecord =
        _pinotHelixResourceManager.getSegmentMetadataZnRecord(tableNameWithType, segmentName);
    if (segmentMetadataZNRecord == null) {
      LOGGER.debug("No ZK metadata for segment: {} of table: {}", segmentName, tableNameWithType);
      return;
    }
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentMetadataZNRecord);
    if (targetTier.getName().equalsIgnoreCase(segmentZKMetadata.getTier())) {
      LOGGER.debug("Segment: {} of table: {} is already on the right tier: {}", segmentName, tableNameWithType,
          targetTier.getName());
      return;
    }
    segmentZKMetadata.setTier(targetTier.getName());
    LOGGER.debug("Segment: {} of table: {} has got new tier: {}", segmentName, tableNameWithType, targetTier.getName());
    _pinotHelixResourceManager
        .updateZkMetadata(tableNameWithType, segmentZKMetadata, segmentMetadataZNRecord.getVersion());
  }
}
