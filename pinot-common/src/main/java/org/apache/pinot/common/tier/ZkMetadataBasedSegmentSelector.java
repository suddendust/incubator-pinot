package org.apache.pinot.common.tier;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;

public class ZkMetadataBasedSegmentSelector implements TierSegmentSelector {

  private final HelixManager _helixManager;

  public ZkMetadataBasedSegmentSelector(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  @Override
  public String getType() {
    return TierFactory.PINOT_DEEP_STORE_STORAGE_TYPE;
  }

  @Override
  public boolean selectSegment(String tableNameWithType, String segmentName) {
    SegmentZKMetadata segmentZKMetadata =
        ZKMetadataProvider.getSegmentZKMetadata(_helixManager.getHelixPropertyStore(), tableNameWithType, segmentName);
    Preconditions
        .checkNotNull(segmentZKMetadata, "Could not find zk metadata for segment: {} of table: {}", segmentName,
            tableNameWithType);
    //if the segment has a valid deep-store URL, it is eligible for selection
    return StringUtils.isNotEmpty(segmentZKMetadata.getDownloadUrl());
  }
}
