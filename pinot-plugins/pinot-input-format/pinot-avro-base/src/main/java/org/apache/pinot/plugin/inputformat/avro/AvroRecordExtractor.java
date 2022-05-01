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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for Avro Records
 */
public class AvroRecordExtractor extends BaseRecordExtractor<GenericRecord> {
  private Set<String> _fields;
  private boolean _extractAll = false;
  private boolean _extractRecordAsJsonBlob = false;

  @Override
  public void init(@Nullable Set<String> fields, boolean extractRecordAsJsonBlob,
      @Nullable RecordExtractorConfig recordExtractorConfig) {
    _extractRecordAsJsonBlob = extractRecordAsJsonBlob;
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Collections.emptySet();
    } else {
      _fields = ImmutableSet.copyOf(fields);
    }
  }

  @Override
  public GenericRow extract(GenericRecord from, GenericRow to) {
    if (_extractRecordAsJsonBlob) {
      to.putValue(BaseRecordExtractor.RECORD_AS_JSON_COL_NAME, from.toString());
      return to;
    }
    if (_extractAll) {
      List<Schema.Field> fields = from.getSchema().getFields();
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        Object value = from.get(fieldName);
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
      }
    } else {
      for (String fieldName : _fields) {
        Object value = from.get(fieldName);
        if (value != null) {
          value = convert(value);
        }
        to.putValue(fieldName, value);
      }
    }
    return to;
  }

  /**
   * Returns whether the object is an Avro GenericRecord.
   */
  @Override
  protected boolean isRecord(Object value) {
    return value instanceof GenericRecord;
  }

  /**
   * Handles the conversion of every field of the Avro GenericRecord.
   *
   * @param value should be verified to be a GenericRecord type prior to calling this method as it will be casted
   *              without checking
   */
  @Override
  @Nullable
  protected Object convertRecord(Object value) {
    GenericRecord record = (GenericRecord) value;
    List<Schema.Field> fields = record.getSchema().getFields();
    if (fields.isEmpty()) {
      return null;
    }

    Map<Object, Object> convertedMap = new HashMap<>();
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object fieldValue = record.get(fieldName);
      if (fieldValue != null) {
        fieldValue = convert(fieldValue);
      }
      convertedMap.put(fieldName, fieldValue);
    }
    return convertedMap;
  }
}
