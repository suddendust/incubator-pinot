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
package org.apache.pinot.common.tier;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.config.table.TierConfig;


/**
 * Factory class to create and sort {@link Tier}
 */
public final class TierFactory {

  public static final String TIME_SEGMENT_SELECTOR_TYPE = "time";
  public static final String METADATA_SEGMENT_SELECTOR_TYPE = "metadata";
  public static final String PINOT_SERVER_STORAGE_TYPE = "pinot_server";
  public static final String PINOT_DEEP_STORE_STORAGE_TYPE = "deep_store";

  private TierFactory() {
  }

  /**
   * Constructs a {@link Tier} from the {@link TierConfig} in the table config
   */
  public static Tier getTier(TierConfig tierConfig, HelixManager helixManager) {
    Map<String, TierSegmentSelector> segmentSelectors = new HashMap<>();
    TierStorage storageSelector;

    String segmentSelectorType = tierConfig.getSegmentSelectorType();
    if (segmentSelectorType.equalsIgnoreCase(TierFactory.TIME_SEGMENT_SELECTOR_TYPE)) {
      segmentSelectors.put(TierFactory.TIME_SEGMENT_SELECTOR_TYPE,
          new TimeBasedTierSegmentSelector(helixManager, tierConfig.getSegmentAge()));
    } else if (segmentSelectorType.equalsIgnoreCase(TierFactory.PINOT_DEEP_STORE_STORAGE_TYPE)) {
      //PINOT_DEEP_STORE_STORAGE_TYPE has two selectors
      segmentSelectors.put(TierFactory.TIME_SEGMENT_SELECTOR_TYPE,
          new TimeBasedTierSegmentSelector(helixManager, tierConfig.getSegmentAge()));
      segmentSelectors.put(TierFactory.METADATA_SEGMENT_SELECTOR_TYPE,
          new ZkMetadataBasedSegmentSelector(helixManager));
    } else {
      throw new IllegalStateException("Unsupported segmentSelectorType: " + segmentSelectorType);
    }

    String storageSelectorType = tierConfig.getStorageType();
    if (storageSelectorType.equalsIgnoreCase(TierFactory.PINOT_SERVER_STORAGE_TYPE)) {
      storageSelector = new PinotServerTierStorage(tierConfig.getServerTag());
    } else if (storageSelectorType.equalsIgnoreCase(TierFactory.PINOT_DEEP_STORE_STORAGE_TYPE)) {
      storageSelector = new DeepStoreTierStorage();
    } else {
      throw new IllegalStateException("Unsupported storageType: " + storageSelectorType);
    }

    return new Tier(tierConfig.getName(), segmentSelectors, storageSelector);
  }
}
