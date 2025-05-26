package org.apache.pinot.spi.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;


public class MultipleTablesAuthorizationResultImpl implements MultipleTablesAuthorizationResult {

  public static final MultipleTablesAuthorizationResult SUCCESS =
      new MultipleTablesAuthorizationResultImpl(Map.of(), Map.of());

  private final Map<String, AuthorizationResult> _tableAuthResults;
  private final Map<String, List<String>> _rlsFilters;
  private final Set<String> _failedTables;

  public MultipleTablesAuthorizationResultImpl(Map<String, AuthorizationResult> tableAuthResults,
      Map<String, List<String>> rlsFilters) {
    _tableAuthResults = tableAuthResults;
    _rlsFilters = rlsFilters;
    _failedTables = _tableAuthResults.entrySet().stream().filter(e -> !e.getValue().hasAccess()).map(a -> a.getKey())
        .collect(Collectors.toSet());
  }

  @Override
  public AuthorizationResult getAuthResult(String tableName) {
    return _tableAuthResults.get(tableName);
  }

  @Override
  public boolean hasAccess() {
    Optional<AuthorizationResult> hasTableWithoutAccessMaybe =
        _tableAuthResults.values().stream().filter(a -> !a.hasAccess()).findFirst();
    return hasTableWithoutAccessMaybe.isEmpty();
  }

  @Override
  public boolean hasAccessForTable(String tableName) {
    AuthorizationResult authorizationResult = _tableAuthResults.get(tableName);
    if (authorizationResult != null) {
      return authorizationResult.hasAccess();
    }
    throw new IllegalArgumentException("Table: " + tableName + " not found!");
  }

  @Override
  public Set<String> getFailedTables() {
    return _failedTables;
  }

  @Override
  public Optional<List<String>> getRLSFiltersForTable(String tableName) {
    return Optional.ofNullable(_rlsFilters.get(tableName));
  }

  @Override
  public Map<String, List<String>> getRLSFiltersForAllTables() {
    return _rlsFilters;
  }

  @Override
  public String getFailureMessage() {
    if (hasAccess()) {
      return StringUtils.EMPTY;
    }

    List<String> failedTablesList = new ArrayList<>(_failedTables);
    Collections.sort(failedTablesList); // Sort to make output deterministic
    return "Authorization Failed for tables: " + failedTablesList;
  }
}
