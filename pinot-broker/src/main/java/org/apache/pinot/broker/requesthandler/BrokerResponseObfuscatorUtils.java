package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.response.BrokerResponse;


public class BrokerResponseObfuscatorUtils {
  public static BrokerResponse obfuscate(BrokerResponse brokerResponse, boolean isExplainPlan, Set<String> rowFilters,
      Set<String> columnsToObfuscate) {
    if (isExplainPlan) {
      List<Object[]> rows = brokerResponse.getResultTable().getRows();
      for (Object[] row : rows) {
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
          row[colIndex] = "OBFUSCATED";
        }
      }
    }
    return brokerResponse;
  }
}
