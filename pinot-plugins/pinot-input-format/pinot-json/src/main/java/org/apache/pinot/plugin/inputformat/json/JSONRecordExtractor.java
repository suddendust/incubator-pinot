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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for JSON records
 */
public class JSONRecordExtractor extends BaseRecordExtractor<Map<String, Object>> {

  private Set<String> _fields;
  private boolean _extractAll = false;
  private boolean _extractEntireRecordAsJsonBlob = false;

  @Override
  public void init(Set<String> fields, boolean extractEntireRecordAsJsonBlob,
      @Nullable RecordExtractorConfig recordExtractorConfig) {
    _extractEntireRecordAsJsonBlob = extractEntireRecordAsJsonBlob;
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Collections.emptySet();
    } else {
      _fields = ImmutableSet.copyOf(fields);
    }
  }

  @Override
  public GenericRow extract(Map<String, Object> from, GenericRow to) {
    if (_extractEntireRecordAsJsonBlob) {
      try {
        to.putValue(BaseRecordExtractor.RECORD_AS_JSON_COL_NAME, new ObjectMapper().writeValueAsString(from));
        return to;
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Error storing JSON record as JSON blob", e);
      }
    }
    if (_extractAll) {
      for (Map.Entry<String, Object> fieldToVal : from.entrySet()) {
        Object value = fieldToVal.getValue();
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldToVal.getKey(), value);
      }
    } else {
      for (String fieldName : _fields) {
        Object value = from.get(fieldName);
        // NOTE about JSON behavior - cannot distinguish between INT/LONG and FLOAT/DOUBLE.
        // DataTypeTransformer fixes it.
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
      }
    }
    return to;
  }
}
