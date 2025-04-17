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
package org.apache.pinot.query;

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
    // Handle column references directly
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

        // Check if the expression contains masked columns
        SqlNode maskedExpr = findAndMaskColumns(expr);

        // If the expression was modified, create a new AS call
        if (maskedExpr != expr) {
          return SqlStdOperatorTable.AS.createCall(
              call.getParserPosition(),
              maskedExpr,
              alias);
        }
      } else {
        // For any other function call, check if it uses masked columns
        SqlNode maskedNode = findAndMaskColumns(call);
        if (maskedNode != call) {
          return maskedNode;
        }
      }
    }

    // For other types of nodes, check if they contain masked columns
    return findAndMaskColumns(node);
  }

  /**
   * Find any masked column references in an expression and replace them with mask() calls.
   * If a function or expression uses a masked column, replace the entire expression with mask(column).
   *
   * @param node The SQL node to check
   * @return The node with masking applied if needed
   */
  private SqlNode findAndMaskColumns(SqlNode node) {
    if (node == null) {
      return null;
    }

    // For column identifiers, apply masking directly
    if (node instanceof SqlIdentifier) {
      return maskIdentifierIfNeeded((SqlIdentifier) node);
    }

    // For calls (functions, operators), check if they contain masked columns
    if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;

      // If this is already a mask function, keep it as is
      if (call.getOperator() instanceof SqlMaskFunction) {
        return call;
      }

      // Check all operands for masked columns
      List<SqlNode> operands = call.getOperandList();
      List<SqlIdentifier> maskedColumnsFound = new ArrayList<>();

      // Collect any masked column identifiers in the operands
      for (SqlNode operand : operands) {
        collectMaskedColumns(operand, maskedColumnsFound);
      }

      // If we found masked columns, replace the entire expression with mask calls
      if (!maskedColumnsFound.isEmpty()) {
        // For simplicity, we'll mask each column individually
        // For more complex expressions, you might want a different strategy
        if (maskedColumnsFound.size() == 1) {
          // If only one masked column, replace with mask(column)
          return _maskOperator.createCall(call.getParserPosition(), maskedColumnsFound.get(0));
        } else {
          // If multiple masked columns, this gets more complex
          // For now, just mask the first one found as a simplification
          // You might want to adjust this strategy based on your needs
          return _maskOperator.createCall(call.getParserPosition(), maskedColumnsFound.get(0));
        }
      }
    }

    // If we didn't find any masked columns or didn't handle this case, return the original node
    return node;
  }

  /**
   * Recursively collect all masked column identifiers in a node.
   *
   * @param node The node to check
   * @param maskedColumns List to collect the masked columns into
   */
  private void collectMaskedColumns(SqlNode node, List<SqlIdentifier> maskedColumns) {
    if (node == null) {
      return;
    }

    if (node instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) node;
      if (shouldMaskIdentifier(id)) {
        maskedColumns.add(id);
      }
    } else if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;
      for (SqlNode operand : call.getOperandList()) {
        collectMaskedColumns(operand, maskedColumns);
      }
    }
  }

  /**
   * Check if an identifier should be masked.
   *
   * @param id The identifier to check
   * @return True if it should be masked
   */
  private boolean shouldMaskIdentifier(SqlIdentifier id) {
    if (id == null || id.names.isEmpty()) {
      return false;
    }

    // Get the column name (last part of the identifier)
    String columnName = id.names.get(id.names.size() - 1);

    // Check if this column should be masked
    return _maskedColumns.contains(columnName);
  }

  /**
   * Apply masking to a column identifier if it's in the masked columns set.
   *
   * @param id The identifier to potentially mask
   * @return The masked or original identifier
   */
  private SqlNode maskIdentifierIfNeeded(SqlIdentifier id) {
    if (shouldMaskIdentifier(id)) {
      // Create a mask() function call with the identifier as its argument
      return _maskOperator.createCall(id.getParserPosition(), id);
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
          "maskVal",                      // Function name
          SqlKind.OTHER_FUNCTION,      // SQL kind
          null,                        // Return type inference
          null,                        // Operand type inference
          null,                        // Operand type checker
          SqlFunctionCategory.USER_DEFINED_FUNCTION); // Function category
    }
  }
}
