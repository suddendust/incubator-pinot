package org.apache.pinot.query.visitor;

import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.query.ModifyFilterClauseVisitor;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Test;


public class ModifyFilterClauseVisitorTest {

  @Test
  public void validateModifyFilterClauseVisitor() {
    String tableName = "employees";
    String originalQuery = "SELECT * FROM employees";
    SqlNodeAndOptions originalQuerySQLNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(originalQuery);
    SqlNode filterNode = getFilterNode(Set.of("department != executive"));
    ModifyFilterClauseVisitor modifyFilterClauseVisitor = new ModifyFilterClauseVisitor(Map.of(tableName, filterNode));
    originalQuerySQLNodeAndOptions.getSqlNode().accept(modifyFilterClauseVisitor);
    //now validate the new query
    String string = ((SqlSelect) (originalQuerySQLNodeAndOptions.getSqlNode())).getWhere().toString();
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(originalQuerySQLNodeAndOptions);
    System.out.println(pinotQuery);
  }

  private SqlNode getFilterNode(Set<String> filters) {
    SqlNode filterNode = null;
    for (String filterExpr : filters) {
      // For each filter like "region='EMEA'", we need to wrap it in a SELECT to parse it
      String dummyQuery = "SELECT * FROM dummy WHERE " + filterExpr;
      SqlNodeAndOptions filterNodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(dummyQuery);
      SqlNode parsedQuery = filterNodeAndOptions.getSqlNode();

      // Extract the WHERE clause from the parsed query
      SqlSelect select = (SqlSelect) parsedQuery;
      SqlNode parsedFilter = select.getWhere();

      if (filterNode == null) {
        filterNode = parsedFilter;
      } else {
        filterNode = SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, filterNode, parsedFilter);
      }
    }
    return filterNode;
  }

}
