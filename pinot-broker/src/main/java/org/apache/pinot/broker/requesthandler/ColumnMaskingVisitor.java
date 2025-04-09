package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * A visitor that applies masking to specified columns in a SQL query.
 * It wraps specified columns in the SELECT list with a mask() function.
 */
public class ColumnMaskingVisitor extends SqlShuttle {
  // Set of column names that should be masked
  private final Set<String> _maskedColumns;

  // Operator for the mask function
  private final SqlMaskFunction _maskOperator;

  /**
   * Constructor with the set of columns to mask.
   *
   * @param maskedColumns Set of column names to be masked
   */
  public ColumnMaskingVisitor(Set<String> maskedColumns) {
    _maskedColumns = maskedColumns;
    // Create a custom operator for the mask function
    _maskOperator = new SqlMaskFunction();
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      // Process the SELECT list first
      SqlNodeList selectList = select.getSelectList();
      SqlNodeList newSelectList = processSelectList(selectList);
      ((SqlSelect) call).setSelectList(newSelectList);
    }
    return super.visit(call);
  }

  /**
   * Process a SELECT list to apply masking to columns.
   *
   * @param selectList The original SELECT list
   * @return A new SELECT list with masked columns
   */
  private SqlNodeList processSelectList(SqlNodeList selectList) {
    // If it's SELECT *, we can't easily transform it
    if (selectList.size() == 1 && selectList.get(0) instanceof SqlIdentifier
        && ((SqlIdentifier) selectList.get(0)).isStar()) {
      return selectList;
    }

    // Create a new SELECT list with masked columns
    List<SqlNode> newSelectItems = new ArrayList<>();

    for (SqlNode item : selectList) {
      newSelectItems.add(applyMaskIfNeeded(item));
    }

    // Create a new SELECT list with the modified items
    return new SqlNodeList(newSelectItems, selectList.getParserPosition());
  }

  /**
   * Apply masking to a SELECT item if needed.
   *
   * @param node The SELECT item node
   * @return The possibly masked SELECT item
   */
  private SqlNode applyMaskIfNeeded(SqlNode node) {
    // Handle column references
    if (node instanceof SqlIdentifier) {
      return maskIdentifierIfNeeded((SqlIdentifier) node);
    }

    // Handle expressions with aliases (AS)
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;

      // Handle aliased expressions: expr AS alias
      if (call.getOperator().getKind() == SqlKind.AS && call.operandCount() == 2) {
        SqlNode expr = call.operand(0);
        SqlNode alias = call.operand(1);

        // Apply mask to the expression part if needed
        SqlNode maskedExpr = applyMaskIfNeeded(expr);

        // If the expression was masked, create a new AS call
        if (maskedExpr != expr) {
          return SqlStdOperatorTable.AS.createCall(
              call.getParserPosition(),
              maskedExpr,
              alias);
        }
      }
    }

    // For other expressions, recursively check and mask any identifiers within
    return node.accept(new SqlShuttle() {
      @Override
      public SqlNode visit(SqlIdentifier id) {
        return maskIdentifierIfNeeded(id);
      }
    });
  }

  /**
   * Apply masking to a column identifier if it's in the masked columns set.
   *
   * @param id The identifier to potentially mask
   * @return The masked or original identifier
   */
  private SqlNode maskIdentifierIfNeeded(SqlIdentifier id) {
    // Skip qualified identifiers with more than two parts (e.g., schema.table.column)
    if (id.names.size() > 2) {
      return id;
    }

    // Get the column name (last part of the identifier)
    String columnName = id.names.get(id.names.size() - 1);

    // Check if this column should be masked
    if (_maskedColumns.contains(columnName)) {
      // Create a mask() function call with the identifier as its argument
      return _maskOperator.createCall(
          id.getParserPosition(),
          id);
    }

    // Column not in masked set, return as is
    return id;
  }

  /**
   * Custom SqlOperator for the mask function.
   */
  private static class SqlMaskFunction extends org.apache.calcite.sql.SqlFunction {
    public SqlMaskFunction() {
      super(
          "reverse",                      // Function name
          SqlKind.OTHER_FUNCTION,      // SQL kind
          null,                        // Return type inference
          null,                        // Operand type inference
          null,                        // Operand type checker
          SqlFunctionCategory.SYSTEM); // Function category
    }
  }
}
