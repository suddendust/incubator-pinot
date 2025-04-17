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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;


public class ModifyFilterClauseVisitor extends SqlBasicVisitor<SqlNode> {
  private final SqlNode _filterNode;

  public ModifyFilterClauseVisitor(SqlNode filterNode) {
    _filterNode = filterNode;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    // Check if this is a SELECT statement
    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      SqlNode where = select.getWhere();

      // Add new filter condition to existing WHERE clause
      if (where != null) {
        SqlNode newWhere = SqlStdOperatorTable.AND.createCall(
            SqlParserPos.ZERO, where, _filterNode);
        select.setWhere(newWhere);
      } else {
        // Set WHERE clause if none exists
        select.setWhere(_filterNode);
      }
    }
    return super.visit(call);
  }
}
