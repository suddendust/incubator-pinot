/** * Licensed to the Apache Software Foundation (ASF) under one * or more contributor license agreements.  See the
 * NOTICE file * distributed with this work for additional information * regarding copyright ownership.  The ASF
 * licenses this file * to you under the Apache License, Version 2.0 (the * "License"); you may not use this file
 * except in compliance * with the License.  You may obtain a copy of the License at * *   http://www.apache
 * .org/licenses/LICENSE-2.0 * * Unless required by applicable law or agreed to in writing, * software distributed
 * under the License is distributed on an * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY * KIND, either
 * express or implied.  See the License for the * specific language governing permissions and limitations * under the
 * License. */
package org.apache.pinot.calcite.rel.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;


/** * PinotMaskColumnRule masks sensitive columns in the query by wrapping them with a maskVal() function. * This
 * rule is applied to columns specified in the constructor. */
public class PinotMaskColumnRule {

  public static class Project extends RelOptRule {
    private final Set<String> _columnsToMask;

    public static final Project INSTANCE =
        new Project(PinotRuleUtils.PINOT_REL_FACTORY, Set.of("event_json", "rsvp_count"));

    /**     * Constructor that takes a set of column names to mask.     */
    public Project(RelBuilderFactory factory, Set<String> columnsToMask) {
      super(operand(LogicalProject.class, any()), factory, null);
      this._columnsToMask = new HashSet<>(columnsToMask);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalProject oldProject = call.rel(0);
      RexBuilder rexBuilder = oldProject.getCluster().getRexBuilder();

      // Get the input relation's field names for proper column resolution
      RelNode input = oldProject.getInput();
      List<String> inputFieldNames = input.getRowType().getFieldNames();

      // First, apply the shuttle to process expressions that include sensitive columns
      ColumnMaskingShuttle maskingShuttle = new ColumnMaskingShuttle(rexBuilder, _columnsToMask, inputFieldNames);
      List<RexNode> updatedProjects = new ArrayList<>();
      for (RexNode node : oldProject.getProjects()) {
        updatedProjects.add(node.accept(maskingShuttle));
      }

      // Then check if any output column names need masking (for aliases)
      List<RexNode> finalProjects = new ArrayList<>();
      List<String> projectNames = oldProject.getRowType().getFieldNames();
      boolean madeChanges = false;

      for (int i = 0; i < updatedProjects.size(); i++) {
        RexNode projExpr = updatedProjects.get(i);
        String projName = projectNames.get(i);

        // If the projection is aliased to a sensitive column name, mask it
        if (_columnsToMask.contains(projName) && !maskingShuttle.isAlreadyMasked(projExpr)) {
          finalProjects.add(maskingShuttle.wrapWithMaskVal(projExpr, projExpr.getType()));
          madeChanges = true;
        } else {
          finalProjects.add(projExpr);
          if (projExpr != oldProject.getProjects().get(i)) {
            madeChanges = true;
          }
        }
      }

      // Only transform if we made changes
      if (madeChanges) {
        LogicalProject newProject =
            oldProject.copy(oldProject.getTraitSet(), oldProject.getInput(), finalProjects, oldProject.getRowType());
        call.transformTo(newProject);
      }
    }
  }

  /**   * A RexShuttle that recursively checks expressions and applies masking to specified columns.   */
  private static class ColumnMaskingShuttle extends RexShuttle {
    private final RexBuilder _rexBuilder;
    private final Set<String> _columnsToMask;
    private final List<String> _inputFieldNames;
    private final Set<RexNode> _maskedNodes = new HashSet<>();

    ColumnMaskingShuttle(RexBuilder rexBuilder, Set<String> columnsToMask, List<String> inputFieldNames) {
      _rexBuilder = rexBuilder;
      _columnsToMask = columnsToMask;
      _inputFieldNames = inputFieldNames;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      // Get the column name for this input reference from the input relation's field names
      int index = inputRef.getIndex();
      if (index >= 0 && index < _inputFieldNames.size()) {
        String columnName = _inputFieldNames.get(index);
        if (_columnsToMask.contains(columnName)) {
          RexNode maskedNode = wrapWithMaskVal(inputRef, inputRef.getType());
          _maskedNodes.add(maskedNode);
          return maskedNode;
        }
      }
      return inputRef;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      String fieldName = fieldAccess.getField().getName();
      if (_columnsToMask.contains(fieldName)) {
        RexNode maskedNode = wrapWithMaskVal(fieldAccess, fieldAccess.getType());
        _maskedNodes.add(maskedNode);
        return maskedNode;
      }
      return super.visitFieldAccess(fieldAccess);
    }

    @Override
    public RexNode visitCall(RexCall call) {
      // First check if this is already a maskVal function before processing operands
      if (isMaskValFunction(call)) {
        _maskedNodes.add(call);
        return call;
      }

      // Visit all operands
      RexCall visitedCall = (RexCall) super.visitCall(call);

      // Check if any operand is in the list to mask directly
      boolean needsMasking = hasColumnToMask(visitedCall);

      if (needsMasking) {
        RexNode maskedNode = wrapWithMaskVal(visitedCall, visitedCall.getType());
        _maskedNodes.add(maskedNode);
        return maskedNode;
      }

      return visitedCall;
    }

    /**     * Check if this expression is already a maskVal function call.     */
    private boolean isMaskValFunction(RexCall call) {
      return call.getOperator() instanceof SqlFunction && ((SqlFunction) call.getOperator()).getName()
          .equals("maskVal");
    }

    /**     * Check if node has already been masked.     */
    public boolean isAlreadyMasked(RexNode node) {
      if (node instanceof RexCall) {
        return isMaskValFunction((RexCall) node);
      }
      return _maskedNodes.contains(node);
    }

    /**     * Check if the expression contains any column that needs masking.     */
    private boolean hasColumnToMask(RexNode node) {
      // First check if node is already masked
      if (_maskedNodes.contains(node)) {
        return false;
      }

      if (node instanceof RexInputRef) {
        int index = ((RexInputRef) node).getIndex();
        if (index >= 0 && index < _inputFieldNames.size()) {
          String columnName = _inputFieldNames.get(index);
          return _columnsToMask.contains(columnName);
        }
        return false;
      } else if (node instanceof RexFieldAccess) {
        String fieldName = ((RexFieldAccess) node).getField().getName();
        return _columnsToMask.contains(fieldName);
      } else if (node instanceof RexCall) {
        // Don't check operands if this is a maskVal function
        if (isMaskValFunction((RexCall) node)) {
          return false;
        }

        RexCall call = (RexCall) node;
        for (RexNode operand : call.getOperands()) {
          if (hasColumnToMask(operand)) {
            return true;
          }
        }
      }
      return false;
    }

    /**     * Wrap the expression with maskVal() function.     */
    public RexNode wrapWithMaskVal(RexNode node, RelDataType returnType) {
      // Create the maskVal function
      SqlFunction maskValFunction = new SqlFunction("maskVal", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR,
          // Return type is same as first argument
          null, OperandTypes.ANY, // Accept any operand type
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

      List<RexNode> operands = new ArrayList<>();
      operands.add(node);
      return _rexBuilder.makeCall(_rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), maskValFunction,
          operands);
    }
  }
}