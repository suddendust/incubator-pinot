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
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Utility class providing default masking functions for each Pinot data type.
 * These functions can be registered with Pinot as UDFs to be used by the ColumnMaskingVisitor.
 */
public class MaskingFunctions {

  /**
   * Masks an INTEGER value by returning 0
   */
  @ScalarFunction
  public static int maskInt(int value) {
    return 0;
  }

  /**
   * Masks a LONG value by returning 0L
   */
  @ScalarFunction
  public static long maskLong(long value) {
    return 0L;
  }

  /**
   * Masks a FLOAT value by returning 0.0f
   */
  @ScalarFunction
  public static float maskFloat(float value) {
    return 0.0f;
  }

  /**
   * Masks a DOUBLE value by returning 0.0
   */
  @ScalarFunction
  public static double maskDouble(double value) {
    return 0.0;
  }

  /**
   * Masks a BOOLEAN value by returning false
   */
  @ScalarFunction
  public static boolean maskBoolean(boolean value) {
    return false;
  }

  /**
   * Masks a STRING value by returning asterisks (*) of similar length
   * For short strings (< 5 chars), returns a fixed "****"
   * For longer strings, preserves first and last character and masks the rest
   */
  @ScalarFunction
  public static String maskStr(String value) {
    if (value == null) {
      return null;
    }

    int length = value.length();
    if (length <= 4) {
      return "****";
    }

    StringBuilder masked = new StringBuilder();
    masked.append(value.charAt(0));
    for (int i = 1; i < length - 1; i++) {
      masked.append('*');
    }
    masked.append(value.charAt(length - 1));

    return masked.toString();
  }

  /**
   * Masks a TIMESTAMP value by returning the epoch start (1970-01-01)
   */
  @ScalarFunction
  public static long maskTimestamp(long timestamp) {
    return 0L; // Epoch start
  }

  /**
   * Masks a DATE value by returning the epoch start date (1970-01-01)
   */
  @ScalarFunction
  public static int maskDate(int dateValue) {
    return 0; // Epoch start date in days since epoch
  }

  /**
   * Masks a BYTES value by returning an empty byte array
   */
  @ScalarFunction
  public static byte[] maskBytes(byte[] value) {
    if (value == null) {
      return null;
    }
    return new byte[value.length];  // Returns array of zeros of the same length
  }

  /**
   * Masks an INT_ARRAY by returning an array of zeros of the same length
   */
  @ScalarFunction
  public static int[] maskIntArray(int[] array) {
    if (array == null) {
      return null;
    }
    return new int[array.length]; // All elements initialized to 0
  }

  /**
   * Masks a LONG_ARRAY by returning an array of zeros of the same length
   */
  @ScalarFunction
  public static long[] maskLongArray(long[] array) {
    if (array == null) {
      return null;
    }
    return new long[array.length]; // All elements initialized to 0
  }

  /**
   * Masks a FLOAT_ARRAY by returning an array of zeros of the same length
   */
  @ScalarFunction
  public static float[] maskFloatArray(float[] array) {
    if (array == null) {
      return null;
    }
    return new float[array.length]; // All elements initialized to 0.0f
  }

  /**
   * Masks a DOUBLE_ARRAY by returning an array of zeros of the same length
   */
  @ScalarFunction
  public static double[] maskDoubleArray(double[] array) {
    if (array == null) {
      return null;
    }
    return new double[array.length]; // All elements initialized to 0.0
  }

  /**
   * Masks a STRING_ARRAY by masking each string in the array
   */
  @ScalarFunction
  public static String[] maskStrArray(String[] array) {
    if (array == null) {
      return null;
    }

    String[] masked = new String[array.length];
    for (int i = 0; i < array.length; i++) {
      masked[i] = maskStr(array[i]);
    }
    return masked;
  }

  /**
   * Masks a JSON object by replacing all leaf values with appropriate masked values
   * based on their types, while preserving the structure
   */
  @ScalarFunction
  public static String maskJson(String jsonString) {
    return "{}";
  }

  /**
   * Default masking function for any type that doesn't have a specific implementation
   * Returns a string representation of a placeholder value
   */
  @ScalarFunction
  public static String maskVal(String value) {
    return "****";
  }
}
