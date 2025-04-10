package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;


public class MaskingFunctions {

  private static final String MASKED_VALUE = "****";

  @ScalarFunction
  public static String mask(String input) {
    return MASKED_VALUE;
  }
}
