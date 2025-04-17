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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.response.BrokerResponse;


//1. Create an internal UDF per data type.
public class BrokerResponseObfuscatorUtils {
  public static BrokerResponse obfuscate(BrokerResponse brokerResponse, boolean isExplainPlan, Set<String> rowFilters,
      Set<String> columnsToObfuscate) {
    if (isExplainPlan) {
      List<Object[]> rows = brokerResponse.getResultTable().getRows();
      for (Object[] row : rows) {
        for (String rowFilter : rowFilters) {
          if (((String) row[0]).contains(rowFilter)) {
            row[0] = ((String) row[0]).replace(rowFilter, "****");
          }
        }
      }
    } else {
      List<Integer> colIndexesToObfuscate = new ArrayList<>();
      String[] columnNames = brokerResponse.getResultTable().getDataSchema().getColumnNames();
      for (int i = 0; i < columnNames.length; i++) {
        String columnName = columnNames[i];
        if (columnsToObfuscate.contains(columnName)) {
          colIndexesToObfuscate.add(i);
        }
      }
      List<Object[]> rows = brokerResponse.getResultTable().getRows();
      for (Object[] row : rows) {
        for (int colIndex : colIndexesToObfuscate) {
          row[colIndex] = "****";
        }
      }
    }
    return brokerResponse;
  }
}
