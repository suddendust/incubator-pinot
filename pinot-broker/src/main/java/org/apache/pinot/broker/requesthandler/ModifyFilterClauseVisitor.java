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
