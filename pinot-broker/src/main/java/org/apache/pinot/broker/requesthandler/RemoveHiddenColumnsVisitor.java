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

import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;


public class RemoveHiddenColumnsVisitor extends SqlBasicVisitor<SqlNode> {
  private final Set<String> columnsToKeep;

  public RemoveHiddenColumnsVisitor(Set<String> columnsToKeep) {
    this.columnsToKeep = columnsToKeep;
  }

  @Override
  public SqlNode visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlCall;
      SqlNodeList selectList = sqlSelect.getSelectList();
      SqlNodeList newSelectList = new SqlNodeList(selectList.getParserPosition());

      for (SqlNode node : selectList) {
        boolean shouldKeep = false;
        for (String colToKeep : columnsToKeep) {
          if (usesColumn(node, colToKeep)) {
            shouldKeep = true;
            break;
          }
        }

        if (shouldKeep) {
          newSelectList.add(node);
        }
      }

      sqlSelect.setSelectList(newSelectList);
    }
    return super.visit(sqlCall);
  }

  /**
   * Recursively checks if the given SqlNode uses the specified column name.
   */
  private boolean usesColumn(SqlNode node, String columnToMatch) {
    if (node instanceof SqlIdentifier) {
      return ((SqlIdentifier) node).getSimple().equalsIgnoreCase(columnToMatch);
    } else if (node instanceof SqlBasicCall || node instanceof SqlCall) {
      for (SqlNode operand : ((SqlCall) node).getOperandList()) {
        if (usesColumn(operand, columnToMatch)) {
          return true;
        }
      }
    }
    return false;
  }
}
