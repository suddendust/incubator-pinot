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

import java.util.Map;


/**
 * Represents a tier of storage in Pinot. It consists of
 * name - unique name given to a tier in the table config and to create instancePartitions for the tier
 * segmentSelector - {@link TierSegmentSelector} strategy used by the tier to select eligible segments of a table
 * storage - {@link TierStorage} used by the tier as storage for the eligible segments
 */
public class Tier {

  private final String _name;
  //key -> selector type, value -> actual selector
  private final Map<String, TierSegmentSelector> _segmentSelectors;
  private final TierStorage _storage;

  public Tier(String name, Map<String, TierSegmentSelector> segmentSelectors, TierStorage storage) {
    _name = name;
    _segmentSelectors = segmentSelectors;
    _storage = storage;
  }

  public String getName() {
    return _name;
  }

  public Map<String, TierSegmentSelector> getSegmentSelectors() {
    return _segmentSelectors;
  }

  public TierStorage getStorage() {
    return _storage;
  }
}
