package org.apache.pinot.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlBasicVisitor;


/**
 * A simplified SQL visitor that validates column references and their data types in filter expressions.
 * Only checks column existence and data type compatibility.
 */
public class FilterExpressionValidator extends SqlBasicVisitor<Boolean> {

  private final Map<String, String> columnTypes;
  private final List<String> validationErrors;

  /**
   * Constructs a validator with a map of column names to their data types.
   *
   * @param columnTypes Map of column names to their corresponding data types as strings
   */
  public FilterExpressionValidator(Map<String, String> columnTypes) {
    this.columnTypes = columnTypes;
    this.validationErrors = new ArrayList<>();
  }

  /**
   * Validates the given SQL node and returns whether it's valid.
   *
   * @param node SQL node to validate
   * @return true if valid, false otherwise
   */
  public boolean validate(SqlNode node) {
    validationErrors.clear();
    return node.accept(this);
  }

  /**
   * Returns the validation errors found during traversal.
   *
   * @return List of validation error messages
   */
  public List<String> getValidationErrors() {
    return validationErrors;
  }

  @Override
  public Boolean visit(SqlLiteral literal) {
    // Literals are always valid by themselves
    return true;
  }

  @Override
  public Boolean visit(SqlIdentifier identifier) {
    String columnName = identifier.getSimple();

    // Validate column existence
    if (!columnTypes.containsKey(columnName)) {
      validationErrors.add("Column '" + columnName + "' does not exist in the table schema");
      return false;
    }

    return true;
  }

  @Override
  public Boolean visit(SqlCall call) {
    boolean isValid = true;

    // First validate all operands recursively
    for (SqlNode operand : call.getOperandList()) {
      if (operand != null) {
        isValid = operand.accept(this) && isValid;
      }
    }

    // Check type compatibility for comparison operations
    if (call instanceof SqlBasicCall) {
      SqlBasicCall basicCall = (SqlBasicCall) call;
      SqlKind kind = basicCall.getKind();

      // For operations that need type checking
      if (isComparisonOperation(kind) || kind == SqlKind.IN || kind == SqlKind.BETWEEN) {
        isValid = checkTypeCompatibility(basicCall) && isValid;
      }
    }

    return isValid;
  }

  /**
   * Determines if the operator is a comparison operator.
   */
  private boolean isComparisonOperation(SqlKind kind) {
    return kind == SqlKind.EQUALS || kind == SqlKind.NOT_EQUALS || kind == SqlKind.LESS_THAN
        || kind == SqlKind.LESS_THAN_OR_EQUAL || kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL
        || kind == SqlKind.LIKE;
  }

  /**
   * Checks type compatibility for comparison operations.
   */
  private boolean checkTypeCompatibility(SqlBasicCall call) {
    SqlKind kind = call.getKind();
    List<SqlNode> operands = call.getOperandList();

    // Handle LIKE separately - it requires string columns
    if (kind == SqlKind.LIKE) {
      return checkLikeTypeCompatibility(operands);
    }

    // Handle standard comparisons and BETWEEN
    if (isComparisonOperation(kind) || kind == SqlKind.BETWEEN) {
      return checkComparisonTypeCompatibility(operands);
    }

    // Handle IN operator
    if (kind == SqlKind.IN) {
      return checkInTypeCompatibility(operands);
    }

    return true;
  }

  /**
   * Checks type compatibility for LIKE operator.
   */
  private boolean checkLikeTypeCompatibility(List<SqlNode> operands) {
    if (operands.size() < 2) {
      return true;
    }

    if (operands.get(0) instanceof SqlIdentifier) {
      SqlIdentifier column = (SqlIdentifier) operands.get(0);
      String columnName = column.getSimple();

      // Only check type compatibility if column exists
      if (columnTypes.containsKey(columnName)) {
        String columnType = columnTypes.get(columnName);

        // LIKE should only be used with string types
        if (!isStringType(columnType)) {
          validationErrors.add(
              "LIKE operator can only be used with string columns: '" + columnName + "' is of type " + columnType);
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Checks type compatibility for comparison operators and BETWEEN.
   */
  private boolean checkComparisonTypeCompatibility(List<SqlNode> operands) {
    if (operands.size() < 2) {
      return true;
    }

    // Extract column and its type
    SqlIdentifier column = null;
    String columnName = null;
    String columnType = null;

    // Try to find a column in the operands
    for (SqlNode operand : operands) {
      if (operand instanceof SqlIdentifier) {
        column = (SqlIdentifier) operand;
        columnName = column.getSimple();

        // Only proceed if column exists
        if (columnTypes.containsKey(columnName)) {
          columnType = columnTypes.get(columnName);
          break;
        }
      }
    }

    // If no valid column found, nothing to check
    if (column == null || columnType == null) {
      return true;
    }

    // Check compatibility with other operands
    for (SqlNode operand : operands) {
      if (operand instanceof SqlLiteral && !operand.equals(column)) {
        SqlLiteral literal = (SqlLiteral) operand;
        String literalType = literal.getTypeName().getName();

        if (!areCompatibleTypes(columnType, literalType)) {
          validationErrors.add(
              "Type mismatch: column '" + columnName + "' is of type " + columnType + " but literal is of type "
                  + literalType);
          return false;
        }
      } else if (operand instanceof SqlIdentifier && !operand.equals(column)) {
        SqlIdentifier otherColumn = (SqlIdentifier) operand;
        String otherColumnName = otherColumn.getSimple();

        // Only check compatibility if the other column exists
        if (columnTypes.containsKey(otherColumnName)) {
          String otherColumnType = columnTypes.get(otherColumnName);

          if (!areCompatibleTypes(columnType, otherColumnType)) {
            validationErrors.add("Type mismatch: column '" + columnName + "' is of type " + columnType + " but column '"
                + otherColumnName + "' is of type " + otherColumnType);
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * Checks type compatibility for IN operator.
   */
  private boolean checkInTypeCompatibility(List<SqlNode> operands) {
    if (operands.size() < 2) {
      return true;
    }

    if (operands.get(0) instanceof SqlIdentifier) {
      SqlIdentifier column = (SqlIdentifier) operands.get(0);
      String columnName = column.getSimple();

      // Only check type compatibility if column exists
      if (columnTypes.containsKey(columnName)) {
        String columnType = columnTypes.get(columnName);

        // Check values in IN list
        if (operands.get(1) instanceof SqlNodeList) {
          SqlNodeList valueList = (SqlNodeList) operands.get(1);

          for (SqlNode value : valueList) {
            if (value instanceof SqlLiteral) {
              SqlLiteral literal = (SqlLiteral) value;
              String literalType = literal.getTypeName().getName();

              if (!areCompatibleTypes(columnType, literalType)) {
                validationErrors.add(
                    "Type mismatch in IN expression: column '" + columnName + "' is of type " + columnType
                        + " but a value is of type " + literalType);
                return false;
              }
            }
          }
        }
      }
    }

    return true;
  }

  /**
   * Checks if two types are compatible for comparison.
   */
  private boolean areCompatibleTypes(String type1, String type2) {
    // Same types are always compatible
    if (type1.equalsIgnoreCase(type2)) {
      return true;
    }

    // Numeric types are compatible with each other
    if (isNumericType(type1) && isNumericType(type2)) {
      return true;
    }

    // String types are compatible with each other
    if (isStringType(type1) && isStringType(type2)) {
      return true;
    }

    // Date/time types are compatible with each other
    if (isDateTimeType(type1) && isDateTimeType(type2)) {
      return true;
    }

    return false;
  }

  /**
   * Checks if a type is a string type.
   */
  private boolean isStringType(String typeName) {
    return typeName.equalsIgnoreCase("CHAR") || typeName.equalsIgnoreCase("VARCHAR") || typeName.equalsIgnoreCase(
        "STRING") || typeName.equalsIgnoreCase("LONGVARCHAR");
  }

  /**
   * Checks if a type is a numeric type.
   */
  private boolean isNumericType(String typeName) {
    return typeName.equalsIgnoreCase("TINYINT") || typeName.equalsIgnoreCase("SMALLINT") || typeName.equalsIgnoreCase(
        "INTEGER") || typeName.equalsIgnoreCase("INT") || typeName.equalsIgnoreCase("BIGINT")
        || typeName.equalsIgnoreCase("DECIMAL") || typeName.equalsIgnoreCase("FLOAT") || typeName.equalsIgnoreCase(
        "REAL") || typeName.equalsIgnoreCase("DOUBLE") || typeName.equalsIgnoreCase("LONG");
  }

  /**
   * Checks if a type is a date/time type.
   */
  private boolean isDateTimeType(String typeName) {
    return typeName.equalsIgnoreCase("DATE") || typeName.equalsIgnoreCase("TIME") || typeName.equalsIgnoreCase(
        "TIMESTAMP");
  }
}