package org.apache.pinot.spi.auth;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Authorization results for multiple tables
 */
public interface MultipleTablesAuthorizationResult {

  /**
   * Gets the authorization result of a particular table
   * @param tableName the table to get the authorization result of
   * @return the corresponding auth result
   */
  AuthorizationResult getAuthResult(String tableName);

  /**
   * Indicates whether overall access is granted.
   *
   * @return true if access is granted for each table, false otherwise.
   */
  boolean hasAccess();

  /**
   * Checks whether a particular table has access
   * @param tableName the table to check for
   * @return true if access is granted, false otherwise
   */
  boolean hasAccessForTable(String tableName);

  /**
   * Returns the set of tables for which access wasn't granted
   * @return the set of tables for which access wasn't granted
   */
  Set<String> getFailedTables();

  /**
   * Get RLS filters for a particular table.
   * @param tableName the table to get RLS filters for
   * @return Optional of the RLS filters
   */
  Optional<List<String>> getRLSFiltersForTable(String tableName);

  Map<String, List<String>> getRLSFiltersForAllTables();

  /**
   * Provides the failure message if access is denied.
   *
   * @return A string containing the failure message if access is denied, otherwise an empty string or null.
   */
  String getFailureMessage();
}
