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
package org.apache.pinot.controller.helix.core.relocation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.helix.ClusterMessagingService;
import org.apache.pinot.common.assignment.InstanceAssignmentConfigUtils;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalanceManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.controller.helix.core.util.MessagingServiceUtils;
import org.apache.pinot.controller.util.TableTierReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.Enablement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Periodic task to run rebalancer in background to:
 * <ol>
 * <li> Relocate COMPLETED segments to tag overrides
 * <li> Relocate ONLINE segments to tiers if tier configs are set
 * </ol>
 * Allow at most one replica unavailable during rebalance. Not applicable for HLC tables.
 */
public class SegmentRelocator extends ControllerPeriodicTask<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentRelocator.class);

  private final TableRebalanceManager _tableRebalanceManager;
  private final ExecutorService _executorService;
  private final HttpClientConnectionManager _connectionManager;
  private final boolean _enableLocalTierMigration;
  private final int _serverAdminRequestTimeoutMs;

  // Rebalance related configs
  private final boolean _reassignInstances;
  private final boolean _bootstrap;
  private final boolean _downtime;
  private final int _minAvailableReplicas;
  private final boolean _bestEfforts;
  private final long _externalViewCheckIntervalInMs;
  private final long _externalViewStabilizationTimeoutInMs;
  private final boolean _includeConsuming;
  private final Enablement _minimizeDataMovement;
  private final int _batchSizePerServer;

  private final Set<String> _waitingTables;
  private final BlockingQueue<String> _waitingQueue;
  @Nullable
  private final Set<String> _tablesUndergoingRebalance;

  public SegmentRelocator(TableRebalanceManager tableRebalanceManager,
      PinotHelixResourceManager pinotHelixResourceManager, LeadControllerManager leadControllerManager,
      ControllerConf config, ControllerMetrics controllerMetrics, ExecutorService executorService,
      HttpClientConnectionManager connectionManager) {
    super(SegmentRelocator.class.getSimpleName(), config.getSegmentRelocatorFrequencyInSeconds(),
        config.getSegmentRelocatorInitialDelayInSeconds(), pinotHelixResourceManager, leadControllerManager,
        controllerMetrics);
    _tableRebalanceManager = tableRebalanceManager;
    _executorService = executorService;
    _connectionManager = connectionManager;
    _enableLocalTierMigration = config.enableSegmentRelocatorLocalTierMigration();
    _serverAdminRequestTimeoutMs = config.getServerAdminRequestTimeoutSeconds() * 1000;

    _reassignInstances = config.getSegmentRelocatorReassignInstances();
    _bootstrap = config.getSegmentRelocatorBootstrap();
    _downtime = config.getSegmentRelocatorDowntime();
    _minAvailableReplicas = config.getSegmentRelocatorMinAvailableReplicas();
    _bestEfforts = config.getSegmentRelocatorBestEfforts();
    // Best effort to let inner part of the task run no longer than the task interval, although not enforced strictly.
    long taskIntervalInMs = config.getSegmentRelocatorFrequencyInSeconds() * 1000L;
    _externalViewCheckIntervalInMs =
        Math.min(taskIntervalInMs, config.getSegmentRelocatorExternalViewCheckIntervalInMs());
    _externalViewStabilizationTimeoutInMs =
        Math.min(taskIntervalInMs, config.getSegmentRelocatorExternalViewStabilizationTimeoutInMs());
    _includeConsuming = config.isSegmentRelocatorIncludingConsuming();
    _minimizeDataMovement = config.getSegmentRelocatorMinimizeDataMovement();
    _batchSizePerServer = config.getSegmentRelocatorBatchSizePerServer();

    if (config.isSegmentRelocatorRebalanceTablesSequentially()) {
      _waitingTables = ConcurrentHashMap.newKeySet();
      _waitingQueue = new LinkedBlockingQueue<>();
      _executorService.submit(() -> {
        LOGGER.info("Rebalance tables sequentially");
        try {
          // Keep checking any table waiting to rebalance.
          while (true) {
            rebalanceWaitingTable(this::rebalanceTable);
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Got interrupted while rebalancing tables sequentially", e);
        }
      });
      _tablesUndergoingRebalance = null;
    } else {
      _waitingTables = null;
      _waitingQueue = null;
      _tablesUndergoingRebalance = ConcurrentHashMap.newKeySet();
    }
  }

  @Override
  protected void processTable(String tableNameWithType) {
    if (_waitingTables == null) {
      assert _tablesUndergoingRebalance != null;
      if (!_tablesUndergoingRebalance.contains(tableNameWithType)) {
        LOGGER.debug("Rebalance table: {} immediately", tableNameWithType);
        _executorService.submit(() -> rebalanceTable(tableNameWithType));
      } else {
        LOGGER.info("The previous rebalance has not yet completed, skip rebalancing table {}", tableNameWithType);
      }
    } else {
      putTableToWait(tableNameWithType);
    }
  }

  @VisibleForTesting
  void putTableToWait(String tableNameWithType) {
    if (_waitingTables.add(tableNameWithType)) {
      _waitingQueue.offer(tableNameWithType);
      LOGGER.debug("Table: {} is added in waiting queue, total waiting: {}", tableNameWithType, _waitingTables.size());
      return;
    }
    LOGGER.debug("Table: {} is already in waiting queue", tableNameWithType);
  }

  @VisibleForTesting
  void rebalanceWaitingTable(Consumer<String> rebalancer)
      throws InterruptedException {
    LOGGER.debug("Getting next waiting table to rebalance");
    String nextTable = _waitingQueue.take();
    try {
      rebalancer.accept(nextTable);
    } finally {
      _waitingTables.remove(nextTable);
      LOGGER.debug("Rebalance done for table: {}, total waiting: {}", nextTable, _waitingTables.size());
    }
  }

  @VisibleForTesting
  BlockingQueue<String> getWaitingQueue() {
    return _waitingQueue;
  }

  private void rebalanceTable(String tableNameWithType) {
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);

    boolean relocate = false;
    if (TierConfigUtils.shouldRelocateToTiers(tableConfig)) {
      relocate = true;
      LOGGER.info("Relocating segments to tiers for table: {}", tableNameWithType);
    }
    if (tableConfig.getTableType() == TableType.REALTIME
        && InstanceAssignmentConfigUtils.shouldRelocateCompletedSegments(tableConfig)) {
      relocate = true;
      LOGGER.info("Relocating COMPLETED segments for table: {}", tableNameWithType);
    }
    if (!relocate) {
      LOGGER.debug("No need to relocate segments of table: {}", tableNameWithType);
      return;
    }

    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setReassignInstances(_reassignInstances);
    rebalanceConfig.setBootstrap(_bootstrap);
    rebalanceConfig.setDowntime(_downtime);
    rebalanceConfig.setMinAvailableReplicas(_minAvailableReplicas);
    rebalanceConfig.setBestEfforts(_bestEfforts);
    rebalanceConfig.setExternalViewCheckIntervalInMs(_externalViewCheckIntervalInMs);
    rebalanceConfig.setExternalViewStabilizationTimeoutInMs(_externalViewStabilizationTimeoutInMs);
    rebalanceConfig.setUpdateTargetTier(TierConfigUtils.shouldRelocateToTiers(tableConfig));
    rebalanceConfig.setIncludeConsuming(_includeConsuming);
    rebalanceConfig.setMinimizeDataMovement(_minimizeDataMovement);
    rebalanceConfig.setBatchSizePerServer(_batchSizePerServer);

    if (_tablesUndergoingRebalance != null) {
      LOGGER.debug("Start rebalancing table: {}, adding to tablesUndergoingRebalance", tableNameWithType);
      if (!_tablesUndergoingRebalance.add(tableNameWithType)) {
        LOGGER.warn("Skip rebalancing table: {}, table already exists in tablesUndergoingRebalance, a rebalance "
            + "must have already been started", tableNameWithType);
        return;
      }
    }
    try {
      // Relocating segments to new tiers needs two sequential actions: table rebalance and local tier migration.
      // Table rebalance moves segments to the new ideal servers, which can change for a segment when its target
      // tier is updated. New servers can put segments onto the right tier when loading the segments. After that,
      // all segments are put on the right servers. If any segments are not on their target tier, the server local
      // tier migration is triggered for them, basically asking the hosting servers to reload them. The segment
      // target tier may get changed between the two sequential actions, but cluster states converge eventually.

      // We're not using the async rebalance API here because we want to run this on a separate thread pool from the
      // rebalance thread pool that is used for user initiated rebalances.

      // Retries are disabled because SegmentRelocator itself is a periodic controller task, so we don't want the
      // RebalanceChecker to unnecessarily retry any such failed rebalances.
      RebalanceResult rebalance = _tableRebalanceManager.rebalanceTable(tableNameWithType, rebalanceConfig,
          TableRebalancer.createUniqueRebalanceJobIdentifier(), false);
      switch (rebalance.getStatus()) {
        case NO_OP:
          LOGGER.info("All segments are already relocated for table: {}", tableNameWithType);
          migrateToTargetTier(tableNameWithType);
          break;
        case DONE:
          LOGGER.info("Finished relocating segments for table: {}", tableNameWithType);
          migrateToTargetTier(tableNameWithType);
          break;
        default:
          LOGGER.error("Relocation failed for table: {}", tableNameWithType);
          break;
      }
    } catch (Throwable t) {
      LOGGER.error("Caught exception/error while rebalancing table: {}", tableNameWithType, t);
    } finally {
      if (_tablesUndergoingRebalance != null) {
        LOGGER.debug("Done rebalancing table: {}, removing from tablesUndergoingRebalance", tableNameWithType);
        _tablesUndergoingRebalance.remove(tableNameWithType);
      }
    }
  }

  /**
   * Migrate segment tiers on their hosting servers locally. Once table is balanced, i.e. segments are on their ideal
   * servers, we check if any segment needs to move to a new tier on its hosting servers, i.e. doing local tier
   * migration for the segments.
   */
  private void migrateToTargetTier(String tableNameWithType) {
    if (!_enableLocalTierMigration) {
      LOGGER.debug("Skipping migrating segments of table: {} to new tiers on hosting servers", tableNameWithType);
      return;
    }
    LOGGER.info("Migrating segments of table: {} to new tiers on hosting servers", tableNameWithType);
    try {
      TableTierReader.TableTierDetails tableTiers =
          new TableTierReader(_executorService, _connectionManager, _pinotHelixResourceManager).getTableTierDetails(
              tableNameWithType, null, _serverAdminRequestTimeoutMs, true);
      triggerLocalTierMigration(tableNameWithType, tableTiers,
          _pinotHelixResourceManager.getHelixZkManager().getMessagingService());
      LOGGER.info("Migrated segments of table: {} to new tiers on hosting servers", tableNameWithType);
    } catch (Exception e) {
      LOGGER.error("Failed to migrate segments of table: {} to new tiers on hosting servers", tableNameWithType, e);
    }
  }

  @VisibleForTesting
  static void triggerLocalTierMigration(String tableNameWithType, TableTierReader.TableTierDetails tableTiers,
      ClusterMessagingService messagingService) {
    Map<String, Map<String, String>> currentTiers = tableTiers.getSegmentCurrentTiers();
    Map<String, String> targetTiers = tableTiers.getSegmentTargetTiers();
    LOGGER.debug("Got segment current tiers: {} and target tiers: {}", currentTiers, targetTiers);
    Map<String, Set<String>> serverToSegmentsToMigrate = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> segmentTiers : currentTiers.entrySet()) {
      String segmentName = segmentTiers.getKey();
      Map<String, String> serverToCurrentTiers = segmentTiers.getValue();
      String targetTier = targetTiers.get(segmentName);
      for (Map.Entry<String, String> serverTier : serverToCurrentTiers.entrySet()) {
        String tier = serverTier.getValue();
        String server = serverTier.getKey();
        if ((tier == null && targetTier == null) || (tier != null && tier.equals(targetTier))) {
          LOGGER.debug("Segment: {} is already on the target tier: {} on server: {}", segmentName,
              TierConfigUtils.normalizeTierName(tier), server);
        } else {
          LOGGER.debug("Segment: {} needs to move from current tier: {} to target tier: {} on server: {}", segmentName,
              TierConfigUtils.normalizeTierName(tier), TierConfigUtils.normalizeTierName(targetTier), server);
          serverToSegmentsToMigrate.computeIfAbsent(server, (s) -> new HashSet<>()).add(segmentName);
        }
      }
    }
    if (!serverToSegmentsToMigrate.isEmpty()) {
      LOGGER.info("Notify servers: {} to move segments to new tiers locally", serverToSegmentsToMigrate.keySet());
      reloadSegmentsForLocalTierMigration(tableNameWithType, serverToSegmentsToMigrate, messagingService);
    } else {
      LOGGER.info("No server needs to move segments to new tiers locally");
    }
  }

  private static void reloadSegmentsForLocalTierMigration(String tableNameWithType,
      Map<String, Set<String>> serverToSegmentsToMigrate, ClusterMessagingService messagingService) {
    for (Map.Entry<String, Set<String>> entry : serverToSegmentsToMigrate.entrySet()) {
      String serverName = entry.getKey();
      List<String> segments = new ArrayList<>(entry.getValue());
      LOGGER.info("Sending SegmentReloadMessage to server: {} to reload segments: {} of table: {}", serverName,
          segments, tableNameWithType);
      SegmentReloadMessage message = new SegmentReloadMessage(tableNameWithType, segments, false);
      int numMessagesSent = MessagingServiceUtils.send(messagingService, message, tableNameWithType, null, serverName);
      if (numMessagesSent > 0) {
        LOGGER.info("Sent SegmentReloadMessage to server: {} for table: {}", serverName, tableNameWithType);
      } else {
        LOGGER.warn("No SegmentReloadMessage sent to server: {} for table: {}", serverName, tableNameWithType);
      }
    }
  }
}
