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
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;

/**
 * Visitor to add filter clauses to SQL queries.
 * Supports both single-table queries and multi-stage join queries by adding
 * filters directly to the WHERE clause with proper column qualification.
 */
public class ModifyFilterClauseVisitor extends SqlBasicVisitor<SqlNode> {
  // Map of table names to their respective filter nodes
  private final Map<String, SqlNode> _tableFilters;

  /**
   * Constructor for multi-stage queries with table-specific filters.
   *
   * @param tableFilters Map of table names to their filter nodes
   */
  public ModifyFilterClauseVisitor(Map<String, SqlNode> tableFilters) {
    _tableFilters = tableFilters;
  }

  /**
   * Constructor for single-stage queries with one filter.
   *
   * @param filterNode Single filter node to apply
   */
  public ModifyFilterClauseVisitor(SqlNode filterNode) {
    this(Map.of("", filterNode)); // Empty key for single table queries
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      handleSelectQuery((SqlSelect) call);
    }
    return super.visit(call);
  }

  /**
   * Handle a SELECT query by collecting all relevant filters
   * and adding them to the WHERE clause.
   */
  private void handleSelectQuery(SqlSelect select) {
    // Get the FROM clause
    SqlNode fromNode = select.getFrom();

    // Collect all filters that apply to this query
    SqlNode additionalFilters = collectFiltersForQuery(fromNode);

    // If we have filters to add, append them to the WHERE clause
    if (additionalFilters != null) {
      SqlNode where = select.getWhere();
      if (where != null) {
        // Add new filters to existing WHERE clause
        SqlNode newWhere = SqlStdOperatorTable.AND.createCall(
            SqlParserPos.ZERO, where, additionalFilters);
        select.setWhere(newWhere);
      } else {
        // Set WHERE clause if none exists
        select.setWhere(additionalFilters);
      }
    }
  }

  /**
   * Collect all filters that apply to tables in this query.
   * For join queries, this includes filters for all tables involved,
   * properly qualified with table aliases.
   *
   * @param fromNode The FROM clause of the query
   * @return A SqlNode representing all collected filters, or null if no filters apply
   */
  private SqlNode collectFiltersForQuery(SqlNode fromNode) {
    if (fromNode instanceof SqlJoin) {
      // Handle JOIN case
      return collectFiltersForJoin((SqlJoin) fromNode);
    } else {
      // Handle single table case
      TableInfo tableInfo = extractTableInfo(fromNode);
      return qualifyFilterForTable(tableInfo);
    }
  }

  /**
   * Collect filters for a JOIN node by getting filters for both sides.
   *
   * @param join The JOIN node
   * @return Combined filters for all tables in the join
   */
  private SqlNode collectFiltersForJoin(SqlJoin join) {
    if (join == null) {
      return null;
    }

    try {
      // Get filters for left side
      SqlNode leftFilters = null;
      if (join.getLeft() instanceof SqlJoin) {
        leftFilters = collectFiltersForJoin((SqlJoin) join.getLeft());
      } else if (join.getLeft() != null) {
        TableInfo leftTable = extractTableInfo(join.getLeft());
        leftFilters = qualifyFilterForTable(leftTable);
      }

      // Get filters for right side
      SqlNode rightFilters = null;
      if (join.getRight() instanceof SqlJoin) {
        rightFilters = collectFiltersForJoin((SqlJoin) join.getRight());
      } else if (join.getRight() != null) {
        TableInfo rightTable = extractTableInfo(join.getRight());
        rightFilters = qualifyFilterForTable(rightTable);
      }

      // Combine filters with AND
      if (leftFilters != null && rightFilters != null) {
        return SqlStdOperatorTable.AND.createCall(
            SqlParserPos.ZERO, leftFilters, rightFilters);
      } else if (leftFilters != null) {
        return leftFilters;
      } else if (rightFilters != null) {
        return rightFilters;
      } else {
        return null;
      }
    } catch (Exception e) {
      // Log error and return null as a fallback
      System.err.println("Error collecting filters for join: " + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Extract table name and alias from a table reference node.
   *
   * @param tableNode The node representing a table in the FROM clause
   * @return TableInfo containing the table name and alias
   */
  private TableInfo extractTableInfo(SqlNode tableNode) {
    String tableName = "";
    String alias = "";

    if (tableNode instanceof SqlIdentifier) {
      // Simple case: FROM tableName
      tableName = ((SqlIdentifier) tableNode).getSimple();
      alias = tableName; // Default alias is the table name itself
    } else if (tableNode instanceof SqlCall) {
      SqlCall tableCall = (SqlCall) tableNode;
      if (tableCall.getOperator().getName().equalsIgnoreCase("AS") &&
          tableCall.getOperandList().size() >= 2) {
        // AS case: FROM tableName AS alias
        SqlNode tableNameNode = tableCall.getOperandList().get(0);
        SqlNode aliasNode = tableCall.getOperandList().get(1);

        if (tableNameNode instanceof SqlIdentifier) {
          tableName = ((SqlIdentifier) tableNameNode).getSimple();
        }

        if (aliasNode instanceof SqlIdentifier) {
          alias = ((SqlIdentifier) aliasNode).getSimple();
        }
      }
    }

    return new TableInfo(tableName, alias);
  }

  /**
   * Create a qualified filter for a specific table.
   *
   * @param tableInfo The table information (name and alias)
   * @return A SqlNode representing the qualified filter, or null if no filter applies
   */
  private SqlNode qualifyFilterForTable(TableInfo tableInfo) {
    if (tableInfo == null || tableInfo.tableName == null || tableInfo.tableName.isEmpty()) {
      return null;
    }

    // Check if we have a filter for this table
    SqlNode filterForTable = _tableFilters.get(tableInfo.tableName);
    if (filterForTable == null) {
      // For single-table case with empty key
      filterForTable = _tableFilters.get("");
      if (filterForTable == null) {
        return null; // No filter applies
      }
    }

    // Perform deep clone of the filter node to avoid modifying the original
    // Note: In actual implementation, you'd use Calcite's clone mechanism
    // This is a conceptual placeholder for that operation
    SqlNode filterCopy = filterForTable; // Placeholder for deep clone

    // Create a copy of the filter with qualified column references
    return qualifyColumnReferences(filterCopy, tableInfo.alias);
  }

  /**
   * Qualify all column references in a filter with the table alias.
   *
   * @param node The SqlNode to qualify
   * @param tableAlias The table alias to use for qualification
   * @return A new SqlNode with qualified column references
   */
  private SqlNode qualifyColumnReferences(SqlNode node, String tableAlias) {
    if (node == null) {
      return null;
    }

    try {
      // Use a visitor to traverse the tree and qualify column references
      ColumnQualifierVisitor qualifier = new ColumnQualifierVisitor(tableAlias);
      SqlNode result = node.accept(qualifier);

      // Verify the result is valid
      if (result == null) {
        System.err.println("Warning: Qualification returned null for node: " + node);
        return node; // Return original as fallback
      }

      return result;
    } catch (Exception e) {
      // If any exception occurs, log it and return the original node as a fallback
      System.err.println("Error qualifying column references: " + e.getMessage());
      e.printStackTrace();
      return node; // Return original node as fallback
    }
  }

  /**
   * Visitor that qualifies column references with a table alias.
   */
  private static class ColumnQualifierVisitor extends SqlBasicVisitor<SqlNode> {
    private final String _tableAlias;

    public ColumnQualifierVisitor(String tableAlias) {
      _tableAlias = tableAlias;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      // If this is a simple column reference (not already qualified),
      // qualify it with the table alias
      if (id == null) {
        return null;
      }

      if (!id.isSimple() || id.names.size() > 1) {
        // Already qualified or not a simple column reference
        return id;
      }

      // Create a new qualified identifier: table.column
      List<String> qualifiedName = new ArrayList<>();
      qualifiedName.add(_tableAlias);
      qualifiedName.add(id.getSimple());

      return new SqlIdentifier(qualifiedName, id.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlCall call) {
      // Check for null call
      if (call == null) {
        return null;
      }

      // For function calls and operators, preserve the original structure
      // Don't attempt to modify the call if we don't need to
      if (call instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) call;
        boolean needsModification = false;

        // Create qualified operands but only if necessary
        List<SqlNode> qualifiedOperands = new ArrayList<>(basicCall.getOperandList().size());
        for (int i = 0; i < basicCall.getOperandList().size(); i++) {
          SqlNode operand = basicCall.getOperandList().get(i);

          // Skip null operands
          if (operand == null) {
            qualifiedOperands.add(null);
            continue;
          }

          // Only try to qualify identifiers or calls that might contain identifiers
          if (operand instanceof SqlIdentifier || operand instanceof SqlCall) {
            SqlNode qualified = operand.accept(this);
            if (qualified != operand) {
              needsModification = true;
            }
            qualifiedOperands.add(qualified);
          } else {
            qualifiedOperands.add(operand);
          }
        }

        // Only create a new call if we actually modified something
        if (needsModification) {
          return basicCall.getOperator().createCall(
              basicCall.getParserPosition(),
              qualifiedOperands);
        } else {
          return basicCall; // Return the original call if no changes were made
        }
      }

      // For other types of calls, return as is
      return call;
    }
  }

  /**
   * Class to hold table name and alias information.
   */
  private static class TableInfo {
    final String tableName;
    final String alias;

    TableInfo(String tableName, String alias) {
      this.tableName = tableName;
      this.alias = alias;
    }
  }
}
