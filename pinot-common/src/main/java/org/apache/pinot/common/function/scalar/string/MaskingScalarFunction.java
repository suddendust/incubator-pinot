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
package org.apache.pinot.common.function.scalar.string;

import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(names = {"maskVal"})
public class MaskingScalarFunction extends PolymorphicMaskingFunction {

  private static final String MASK_VALUE = "****";
  private static final int NUMERIC_MASK_VALUE = -999; // Numeric mask value

  @Override
  public String getName() {
    return "maskVal";
  }

  @Override
  protected FunctionInfo functionInfoForType(DataSchema.ColumnDataType argumentType) {
    try {
      switch (argumentType) {
        case STRING:
          return new FunctionInfo(MaskingScalarFunction.class.getMethod("maskString", String.class), MaskingScalarFunction.class, false);
        case INT:
          return new FunctionInfo(
              MaskingScalarFunction.class.getMethod("maskInt", Integer.class), MaskingScalarFunction.class, false);
        case LONG:
          return new FunctionInfo(MaskingScalarFunction.class.getMethod("maskLong", Long.class), MaskingScalarFunction.class, false);
        case FLOAT:
          return new FunctionInfo(
              MaskingScalarFunction.class.getMethod("maskFloat", Float.class), MaskingScalarFunction.class, false);
        case DOUBLE:
          return new FunctionInfo(MaskingScalarFunction.class.getMethod("maskDouble", Double.class), MaskingScalarFunction.class, false);
        default:
          // Default to string masking for other types
          return new FunctionInfo(MaskingScalarFunction.class.getMethod("maskString", String.class), MaskingScalarFunction.class, false);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Masks a string value with asterisks
   */
  @ScalarFunction
  public static String maskString(String value) {
    if (value == null) {
      return null;
    }
    return MASK_VALUE;
  }

  /**
   * Masks an integer value with a masking number
   */
  @ScalarFunction
  public static Integer maskInt(Integer value) {
    if (value == null) {
      return null;
    }
    return NUMERIC_MASK_VALUE;
  }

  /**
   * Masks a long value with a masking number
   */
  @ScalarFunction
  public static Long maskLong(Long value) {
    if (value == null) {
      return null;
    }
    return (long) NUMERIC_MASK_VALUE;
  }

  /**
   * Masks a float value with a masking number
   */
  @ScalarFunction
  public static Float maskFloat(Float value) {
    if (value == null) {
      return null;
    }
    return (float) NUMERIC_MASK_VALUE;
  }

  /**
   * Masks a double value with a masking number
   */
  @ScalarFunction
  public static Double maskDouble(Double value) {
    if (value == null) {
      return null;
    }
    return (double) NUMERIC_MASK_VALUE;
  }

  /**
   * Masks any other type by converting to string and returning mask value
   */
  @ScalarFunction
  public static String maskToString(Object value) {
    if (value == null) {
      return null;
    }
    return MASK_VALUE;
  }
}
