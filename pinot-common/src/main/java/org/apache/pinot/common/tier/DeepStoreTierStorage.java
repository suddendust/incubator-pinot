package org.apache.pinot.common.tier;

public class DeepStoreTierStorage implements TierStorage {

  public DeepStoreTierStorage() {
  }

  @Override
  public String getType() {
    return TierFactory.PINOT_DEEP_STORE_STORAGE_TYPE;
  }
}
