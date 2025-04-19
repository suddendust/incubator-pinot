/** * Licensed to the Apache Software Foundation (ASF) under one * or more contributor license agreements.  See the
 * NOTICE file * distributed with this work for additional information * regarding copyright ownership.  The ASF
 * licenses this file * to you under the Apache License, Version 2.0 (the * "License"); you may not use this file
 * except in compliance * with the License.  You may obtain a copy of the License at * *   http://www.apache
 * .org/licenses/LICENSE-2.0 * * Unless required by applicable law or agreed to in writing, * software distributed
 * under the License is distributed on an * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY * KIND, either
 * express or implied.  See the License for the * specific language governing permissions and limitations * under the
 * License. */
package org.apache.pinot.calcite.rel.rules;

import java.math.BigDecimal;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;


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

      // Create a tracking set to avoid infinite recursion
      Set<RexNode> processedNodes = new HashSet<>();

      // Check if any output column needs masking
      List<String> projectNames = oldProject.getRowType().getFieldNames();
      boolean anyColumnToMask = false;
      for (String name : projectNames) {
        if (_columnsToMask.contains(name)) {
          anyColumnToMask = true;
          break;
        }
      }

      if (!anyColumnToMask) {
        return; // No columns to mask, exit early
      }

      // Process each projection
      List<RexNode> finalProjects = new ArrayList<>();
      boolean madeChanges = false;

      for (int i = 0; i < oldProject.getProjects().size(); i++) {
        RexNode expr = oldProject.getProjects().get(i);
        String name = projectNames.get(i);

        if (_columnsToMask.contains(name)) {
          // Get the type of this projection
          RelDataType dataType = expr.getType();

          // Create an appropriate masked value based on the data type
          RexNode maskedValue = createMaskedValue(rexBuilder, dataType);
          finalProjects.add(maskedValue);
          madeChanges = true;
        } else {
          finalProjects.add(expr);
        }
      }

      if (madeChanges) {
        // Create a new project with the same row type but masked values
        LogicalProject newProject = oldProject.copy(oldProject.getTraitSet(), oldProject.getInput(), finalProjects,
            oldProject.getRowType());  // Keep original row type

        call.transformTo(newProject);
      }
    }

    /**
     * Create a masked value that's compatible with the given data type
     */
    private RexNode createMaskedValue(RexBuilder rexBuilder, RelDataType dataType) {
      SqlTypeName typeName = dataType.getSqlTypeName();

      switch (typeName) {
        case INTEGER:
        case TINYINT:
        case SMALLINT:
          return rexBuilder.makeExactLiteral(BigDecimal.ZERO);

        case BIGINT:
          return rexBuilder.makeBigintLiteral(BigDecimal.ZERO);

        case FLOAT:
        case REAL:
        case DOUBLE:
        case DECIMAL:
          return rexBuilder.makeApproxLiteral(BigDecimal.ZERO);

        case BOOLEAN:
          return rexBuilder.makeLiteral(false);

        case DATE:
          return rexBuilder.makeDateLiteral(new DateString("1970-01-01"));

        case TIME:
          return rexBuilder.makeTimeLiteral(new TimeString("00:00:00"), 0);

        case TIMESTAMP:
          return rexBuilder.makeTimestampLiteral(new TimestampString("1970-01-01 00:00:00"), 0);

        case CHAR:
        case VARCHAR:
        default:
          // For string types or any other type, use "****"
          return rexBuilder.makeLiteral("****");
      }
    }
  }

  /**   * A RexShuttle that recursively checks expressions and applies masking to specified columns.   */
  private static class ColumnMaskingShuttle extends RexShuttle {
    private final RexBuilder _rexBuilder;
    private final Set<String> _columnsToMask;
    private final List<String> _inputFieldNames;
    private final Set<RexNode> _processedNodes;
    private final Set<RexNode> _maskedNodes = new HashSet<>();

    ColumnMaskingShuttle(RexBuilder rexBuilder, Set<String> columnsToMask, List<String> inputFieldNames,
        Set<RexNode> processedNodes) {
      _rexBuilder = rexBuilder;
      _columnsToMask = columnsToMask;
      _inputFieldNames = inputFieldNames;
      _processedNodes = processedNodes;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      // Skip if already processed
      if (_processedNodes.contains(inputRef)) {
        return inputRef;
      }
      _processedNodes.add(inputRef);

      // Get the column name for this input reference
      int index = inputRef.getIndex();
      if (index >= 0 && index < _inputFieldNames.size()) {
        String columnName = _inputFieldNames.get(index);
        if (_columnsToMask.contains(columnName)) {
          RexNode maskedNode = createMaskLiteral();
          _maskedNodes.add(maskedNode);
          return maskedNode;
        }
      }
      return inputRef;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      // Skip if already processed
      if (_processedNodes.contains(fieldAccess)) {
        return fieldAccess;
      }
      _processedNodes.add(fieldAccess);

      String fieldName = fieldAccess.getField().getName();
      if (_columnsToMask.contains(fieldName)) {
        RexNode maskedNode = createMaskLiteral();
        _maskedNodes.add(maskedNode);
        return maskedNode;
      }
      return super.visitFieldAccess(fieldAccess);
    }

    @Override
    public RexNode visitCall(RexCall call) {
      // Skip if already processed
      if (_processedNodes.contains(call)) {
        return call;
      }
      _processedNodes.add(call);

      // First check if this is already a maskVal function
      if (isMaskValFunction(call)) {
        _maskedNodes.add(call);
        return call;
      }

      // Visit all operands
      RexCall visitedCall = (RexCall) super.visitCall(call);
      return visitedCall;
    }

    /**
     * Check if this expression is already a maskVal function call.
     */
    private boolean isMaskValFunction(RexCall call) {
      return call.getOperator() instanceof SqlFunction && ((SqlFunction) call.getOperator()).getName()
          .equals("maskVal");
    }

    /**
     * Check if node has already been masked.
     */
    public boolean isNodeMasked(RexNode node) {
      return _maskedNodes.contains(node) || (node instanceof RexCall && isMaskValFunction((RexCall) node));
    }

    /**
     * Create a literal mask value "****".
     */
    public RexNode createMaskLiteral() {
      return _rexBuilder.makeLiteral("****");
    }
  }
}