package org.apache.pinot.common.metrics;

import com.google.common.base.Objects;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricName;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerMetricsTest {
  private ServerMetrics _serverMetrics;

  private HttpClient _httpClient;

  private static final List<String> METER_TYPES =
      List.of("Count", "FiveMinuteRate", "MeanRate", "OneMinuteRate", "FifteenMinuteRate");

  private static final List<String> EXPORTED_METER_PREFIXES =
      List.of("pinot_server_numResizes", "pinot_server_upsertOutOfOrder", "pinot_server_queriesKilled",
          "pinot_server_llcControllerResponse_NotSent", "pinot_server_incompleteRealtimeRowsConsumed",
          "pinot_server_numSecondaryQueriesScheduled", "pinot_server_llcControllerResponse_UploadSuccess",
          "pinot_server_numSegmentsPrunedInvalid", "pinot_server_segmentUploadTimeout",
          "pinot_server_nettyConnection_ResponsesSent", "pinot_server_realtime_exceptions_serverOutOfCapacity",
          "pinot_server_segmentDownloadFromRemoteFailures", "pinot_server_metadataTtlPrimaryKeysRemoved",
          "pinot_server_segmentDirMovementFailures", "pinot_server_numMissingSegments",
          "pinot_server_realtime_exceptions_requestDeserialization", "pinot_server_segmentDownloadFailures",
          "pinot_server_totalKeysMarkedForDeletion", "pinot_server_helix_zookeeperReconnects",
          "pinot_server_resizeTimeMs", "pinot_server_llcControllerResponse_Failed", "pinot_server_rowsWithErrors",
          "pinot_server_realtime_offsetCommits", "pinot_server_realtime_consumptionExceptions",
          "pinot_server_llcControllerResponse_NotLeader", "pinot_server_queryExecutionExceptions",
          "pinot_server_realtime_exceptions_queryExecution", "pinot_server_deletedTtlKeysInMultipleSegments",
          "pinot_server_readinessCheckOkCalls", "pinot_server_hashJoinTimesMaxRowsReached",
          "pinot_server_largeQueryResponseSizeExceptions", "pinot_server_upsertMissedValidDocIdSnapshotCount",
          "pinot_server_realtime_exceptions_responseSerialization", "pinot_server_readinessCheckBadCalls",
          "pinot_server_multiStageRawMessages", "pinot_server_heapPanicLevelExceeded",
          "pinot_server_grpcTransportReady", "pinot_server_realtimeRowsFiltered", "pinot_server_numSegmentsProcessed",
          "pinot_server_aggregateTimesNumGroupsLimitReached", "pinot_server_numDocsScanned", "pinot_server_grpcQueries",
          "pinot_server_partialUpsertKeysNotReplaced", "pinot_server_reloadFailures",
          "pinot_server_realtimeRowsConsumed", "pinot_server_llcControllerResponse_Keep",
          "pinot_server_realtimeDedupDropped", "pinot_server_partialUpsertOutOfOrder", "pinot_server_queries",
          "pinot_server_indexingFailures", "pinot_server_realtime_exceptions_streamConsumerCreate",
          "pinot_server_largeQueryResponsesSent", "pinot_server_realtimePartitionMismatch",
          "pinot_server_segmentUploadFailure", "pinot_server_heapCriticalLevelExceeded",
          "pinot_server_numEntriesScannedInFilter", "pinot_server_deletedSegmentCount",
          "pinot_server_numSecondaryQueries", "pinot_server_realtimeRowsFetched",
          "pinot_server_segmentDownloadFromPeersFailures", "pinot_server_realtime_exceptions_realtimeOffsetCommit",
          "pinot_server_llcControllerResponse_CatchUp", "pinot_server_grpcBytesSent",
          "pinot_server_realtime_exceptions_uncaught", "pinot_server_invalidRealtimeRowsDropped",
          "pinot_server_llcControllerResponse_CommitSuccess", "pinot_server_numSegmentsPrunedByLimit",
          "pinot_server_upsertPreloadFailure", "pinot_server_llcControllerResponse_Processed",
          "pinot_server_upsertKeysInWrongSegment", "pinot_server_llcControllerResponse_Hold",
          "pinot_server_numSegmentsQueried", "pinot_server_realtime_exceptions_schedulingTimeout",
          "pinot_server_windowTimesMaxRowsReached", "pinot_server_deleteTableFailures", "pinot_server_noTableAccess",
          "pinot_server_deletedKeysTtlPrimaryKeysRemoved", "pinot_server_segmentStreamedDownloadUntarFailures",
          "pinot_server_totalThreadCpuTimeMillis", "pinot_server_deletedKeysWithinTtlWindow",
          "pinot_server_numSegmentsMatched", "pinot_server_nettyConnection_BytesReceived",
          "pinot_server_numEntriesScannedPostFilter", "pinot_server_nettyConnection_BytesSent",
          "pinot_server_schedulingTimeoutExceptions", "pinot_server_realtime_rowsConsumed",
          "pinot_server_realtimeMergedTextIdxTruncatedDocumentSize", "pinot_server_refreshFailures",
          "pinot_server_numSegmentsPrunedByValue", "pinot_server_untarFailures", "pinot_server_grpcTransportTerminated",
          "pinot_server_multiStageRawBytes", "pinot_server_llcControllerResponse_Commit",
          "pinot_server_llcControllerResponse_Discard", "pinot_server_realtime_exceptions_largeQueryResponseSize",
          "pinot_server_realtimeRowsSanitized", "pinot_server_realtimeConsumptionExceptions",
          "pinot_server_llcControllerResponse_CommitContinue", "pinot_server_segmentUploadSuccess",
          "pinot_server_grpcBytesReceived", "pinot_server_multiStageInMemoryMessages");

  @BeforeClass
  public void setup() {
    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ServerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    _serverMetrics = new ServerMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    JmxReporter jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();

    _httpClient = new HttpClient();
  }

  @Test
  public void serverMeterTest2()
      throws URISyntaxException, IOException {

    String rawTableName = "myTable";
    String tableNameWithType = "myTable" + "_" + "REALTIME";
    String clientId = tableNameWithType + "-" + "myTopic" + "-" + "myPartitionGroupId" + "-" + "myClientId";
    String tableStreamName = tableNameWithType + "_" + "myStreamTopic";

    //todo: Add test for NUM_SECONDARY_QUERIES as it's not used anywhere currently

    //todo: Add test for SERVER_OUT_OF_CAPACITY_EXCEPTIONS as it's not used anywhere currently

    addGlobalMeter(ServerMeter.QUERIES);
    assertMetricExportedCorrectly("pinot_server_queries");

    addGlobalMeter(ServerMeter.UNCAUGHT_EXCEPTIONS);
    assertMetricExportedCorrectly("pinot_server_realtime_exceptions_uncaught");

    addGlobalMeter(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS);
    assertMetricExportedCorrectly("pinot_server_realtime_exceptions_requestDeserialization");

    addGlobalMeter(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS);
    assertMetricExportedCorrectly("pinot_server_realtime_exceptions_responseSerialization");

    addGlobalMeter(ServerMeter.QUERY_EXECUTION_EXCEPTIONS);
    assertMetricExportedCorrectly("pinot_server_realtime_exceptions_queryExecution");

    addGlobalMeter(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS);
    assertMetricExportedCorrectly("pinot_server_helix_zookeeperReconnects");

    addGlobalMeter(ServerMeter.REALTIME_ROWS_CONSUMED);
    assertMetricExportedCorrectly("pinot_server_realtime_rowsConsumed");

    addGlobalMeter(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS);
    assertMetricExportedCorrectly("pinot_server_realtime_consumptionExceptions");

    addMeterWithLables(ServerMeter.REALTIME_ROWS_CONSUMED, clientId);
    addMeterWithLables(ServerMeter.REALTIME_ROWS_SANITIZED, clientId);
    addMeterWithLables(ServerMeter.REALTIME_ROWS_FETCHED, clientId);
    addMeterWithLables(ServerMeter.REALTIME_ROWS_FILTERED, clientId);
    addMeterWithLables(ServerMeter.INVALID_REALTIME_ROWS_DROPPED, clientId);
    addMeterWithLables(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED, clientId);
    addMeterWithLables(ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, clientId);

    //todo: REALTIME_OFFSET_COMMITS, REALTIME_OFFSET_COMMIT_EXCEPTIONS

    addMeterWithLables(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, tableStreamName);

    addMeterWithLables(ServerMeter.REALTIME_MERGED_TEXT_IDX_TRUNCATED_DOCUMENT_SIZE, rawTableName);

    addMeterWithLables(ServerMeter.QUERIES, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_SECONDARY_QUERIES_SCHEDULED, tableNameWithType);
    addMeterWithLables(ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, tableNameWithType);
    addMeterWithLables(ServerMeter.QUERY_EXECUTION_EXCEPTIONS, tableNameWithType);
    addMeterWithLables(ServerMeter.DELETED_SEGMENT_COUNT, tableNameWithType);
    addMeterWithLables(ServerMeter.DELETE_TABLE_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.REALTIME_PARTITION_MISMATCH, tableNameWithType);
    addMeterWithLables(ServerMeter.REALTIME_DEDUP_DROPPED, tableNameWithType);
    addMeterWithLables(ServerMeter.UPSERT_KEYS_IN_WRONG_SEGMENT, tableNameWithType);
    addMeterWithLables(ServerMeter.UPSERT_OUT_OF_ORDER, tableNameWithType);
    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED, tableNameWithType);
    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER, tableNameWithType);
    addMeterWithLables(ServerMeter.DELETED_KEYS_TTL_PRIMARY_KEYS_REMOVED, tableNameWithType);
    addMeterWithLables(ServerMeter.TOTAL_KEYS_MARKED_FOR_DELETION, tableNameWithType);
    addMeterWithLables(ServerMeter.DELETED_KEYS_WITHIN_TTL_WINDOW, tableNameWithType);
    addMeterWithLables(ServerMeter.DELETED_TTL_KEYS_IN_MULTIPLE_SEGMENTS, tableNameWithType);
    addMeterWithLables(ServerMeter.METADATA_TTL_PRIMARY_KEYS_REMOVED, tableNameWithType);
    addMeterWithLables(ServerMeter.UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT, tableNameWithType);
    addMeterWithLables(ServerMeter.UPSERT_PRELOAD_FAILURE, tableNameWithType);

    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, clientId);
    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, tableStreamName);

    addMeterWithLables(ServerMeter.NUM_DOCS_SCANNED, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_IN_FILTER, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_POST_FILTER, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_SEGMENTS_QUERIED, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PROCESSED, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_SEGMENTS_MATCHED, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_MISSING_SEGMENTS, tableNameWithType);
    addMeterWithLables(ServerMeter.RELOAD_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.REFRESH_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.UNTAR_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.SEGMENT_STREAMED_DOWNLOAD_UNTAR_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.SEGMENT_DIR_MOVEMENT_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_REMOTE_FAILURES, tableNameWithType);
    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_PEERS_FAILURES, tableNameWithType);

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_FAILURE, rawTableName);
    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_SUCCESS, rawTableName);
    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_TIMEOUT, rawTableName);

    addMeterWithLables(ServerMeter.NUM_RESIZES, tableNameWithType);
    addMeterWithLables(ServerMeter.RESIZE_TIME_MS, tableNameWithType);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_INVALID, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_LIMIT, tableNameWithType);
    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_VALUE, tableNameWithType);
    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSES_SENT, tableNameWithType);
    addMeterWithLables(ServerMeter.TOTAL_THREAD_CPU_TIME_MILLIS, tableNameWithType);

    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, tableNameWithType);

    //now, add all meters globally
    for (ServerMeter serverMeter : ServerMeter.values()) {
      _serverMetrics.addMeteredGlobalValue(serverMeter, 5L);
    }

  }

  private void assertMetricExportedCorrectly(String exportedMeterPrefix)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    for (String meterType : METER_TYPES) {
      Assert.assertTrue(promMetrics.contains(PromMetric.withName(exportedMeterPrefix + "_" + meterType)));
    }
  }

  private SimpleHttpResponse getExportedPromMetrics()
      throws IOException, URISyntaxException {
    return _httpClient.sendGetRequest(new URI("http://localhost:9021/metrics"));
  }

  @Test
  public void serverMeterTest()
      throws IOException, URISyntaxException {
    //first, validate all global metrics
    List<ServerMeter> GLOBAL_METERS_USED_LOCALLY =
        List.of(ServerMeter.QUERIES, ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, ServerMeter.REALTIME_ROWS_CONSUMED,
            ServerMeter.REALTIME_ROWS_SANITIZED, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS,
            ServerMeter.INDEXING_FAILURES);
    for (ServerMeter serverMeter : ServerMeter.values()) {
      if (serverMeter.isGlobal()) {
        //all global meters use
        _serverMetrics.addMeteredGlobalValue(serverMeter, 5L);
        if (GLOBAL_METERS_USED_LOCALLY.contains(serverMeter)) {
//          _serverMetrics.addMeteredTableValue("myTable", );
        }
        String meterName = serverMeter.getMeterName();
        String fullMeterName = "pinot.server." + meterName;
        final PinotMetricName metricName = PinotMetricUtils.makePinotMetricName(ServerMetrics.class, fullMeterName);
        SimpleHttpResponse response = _httpClient.sendGetRequest(new URI("http://localhost:9021/metrics"));
        List<PromMetric> promMetrics = parseExportedPromMetrics(response.getResponse());

        String promExportedMetricPrefix = ((YammerMetricName) metricName).getMetricName().getName().replace('.', '_');
        //Count, FiveMinuteRate, MeanRate, OneMinuteRate, FifteenMinuteRate
        for (String metricType : METER_TYPES) {
          PromMetric promMetric = PromMetric.withName(promExportedMetricPrefix + "_" + metricType);
          boolean contains = promMetrics.contains(promMetric);
        }
      }
    }

    SimpleHttpResponse response = _httpClient.sendGetRequest(new URI("http://localhost:9021/metrics"));
    List<PromMetric> promMetrics = parseExportedPromMetrics(response.getResponse());

    Set<String> exportedMetricNamePrefixes =
        Set.of("pinot_server_windowTimesMaxRowsReached", "pinot_server_readinessCheckBadCalls",
            "pinot_server_queriesKilled", "pinot_server_llcControllerResponse_NotSent",
            "pinot_server_multiStageRawMessages", "pinot_server_heapPanicLevelExceeded", "pinot_server_noTableAccess",
            "pinot_server_heapCriticalLevelExceeded", "pinot_server_grpcTransportReady",
            "pinot_server_llcControllerResponse_UploadSuccess", "pinot_server_nettyConnection_BytesReceived",
            "pinot_server_nettyConnection_ResponsesSent", "pinot_server_aggregateTimesNumGroupsLimitReached",
            "pinot_server_nettyConnection_BytesSent", "pinot_server_grpcQueries",
            "pinot_server_llcControllerResponse_CatchUp", "pinot_server_realtime_exceptions_requestDeserialization",
            "pinot_server_realtime_rowsConsumed", "pinot_server_helix_zookeeperReconnects",
            "pinot_server_grpcBytesSent", "pinot_server_realtime_exceptions_uncaught",
            "pinot_server_llcControllerResponse_Failed", "pinot_server_grpcTransportTerminated",
            "pinot_server_llcControllerResponse_CommitSuccess", "pinot_server_realtime_offsetCommits",
            "pinot_server_multiStageRawBytes", "pinot_server_realtime_consumptionExceptions",
            "pinot_server_llcControllerResponse_Commit", "pinot_server_llcControllerResponse_NotLeader",
            "pinot_server_llcControllerResponse_Processed", "pinot_server_llcControllerResponse_Discard",
            "pinot_server_readinessCheckOkCalls", "pinot_server_hashJoinTimesMaxRowsReached",
            "pinot_server_realtimeRowsSanitized", "pinot_server_llcControllerResponse_Keep",
            "pinot_server_llcControllerResponse_CommitContinue", "pinot_server_llcControllerResponse_Hold",
            "pinot_server_grpcBytesReceived", "pinot_server_realtime_exceptions_schedulingTimeout",
            "pinot_server_queries", "pinot_server_realtime_exceptions_responseSerialization",
            "pinot_server_indexingFailures", "pinot_server_multiStageInMemoryMessages");

    for (String exportedMetricNamePrefix : exportedMetricNamePrefixes) {
      for (String metricType : METER_TYPES) {
        String exportedMetricName = exportedMetricNamePrefix + "_" + metricType;
        Assert.assertTrue(promMetrics.contains(PromMetric.withName(exportedMetricName)));
      }
    }

    String tableName = "myTable";
    String tableNameWithType = "myTable_REALTIME";

    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES_SCHEDULED, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.QUERY_EXECUTION_EXCEPTIONS, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.DELETED_SEGMENT_COUNT, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.DELETE_TABLE_FAILURES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.TOTAL_THREAD_CPU_TIME_MILLIS, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);
    _serverMetrics.addMeteredTableValue(tableName, ServerMeter.NUM_SECONDARY_QUERIES, 5L);

    //tableNameWithType
//    NUM_SECONDARY_QUERIES("queries", false),
//    NUM_SECONDARY_QUERIES_SCHEDULED("queries", false),
//    QUERY_EXECUTION_EXCEPTIONS("exceptions", false),
//    DELETED_SEGMENT_COUNT("segments", false),
//    DELETE_TABLE_FAILURES("tables", false),
//    LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS("exceptions", false),
//    TOTAL_THREAD_CPU_TIME_MILLIS("millis", false),
//    LARGE_QUERY_RESPONSES_SENT("largeResponses", false),
//    NUM_SEGMENTS_PRUNED_INVALID("numSegmentsPrunedInvalid", false),
//        NUM_SEGMENTS_PRUNED_BY_LIMIT("numSegmentsPrunedByLimit", false),
//        NUM_SEGMENTS_PRUNED_BY_VALUE("numSegmentsPrunedByValue", false),
//    NUM_RESIZES("numResizes", false),
//        RESIZE_TIME_MS("resizeTimeMs", false),
//    SEGMENT_DIR_MOVEMENT_FAILURES("segments", false),
//        SEGMENT_DOWNLOAD_FAILURES("segments", false),
//        SEGMENT_DOWNLOAD_FROM_REMOTE_FAILURES("segments", false),
//        SEGMENT_DOWNLOAD_FROM_PEERS_FAILURES("segments", false),
//    NUM_MISSING_SEGMENTS("segments", false),
//        RELOAD_FAILURES("segments", false),
//        REFRESH_FAILURES("segments", false),
//        UNTAR_FAILURES("segments", false),
//        SEGMENT_STREAMED_DOWNLOAD_UNTAR_FAILURES("segments", false, "Counts the number of segment "
//            + "fetch failures"),
//    NUM_DOCS_SCANNED("rows", false),
//        NUM_ENTRIES_SCANNED_IN_FILTER("entries", false),
//        NUM_ENTRIES_SCANNED_POST_FILTER("entries", false),
//        NUM_SEGMENTS_QUERIED("numSegmentsQueried", false),
//        NUM_SEGMENTS_PROCESSED("numSegmentsProcessed", false),
//        NUM_SEGMENTS_MATCHED("numSegmentsMatched", false),
//    DELETED_KEYS_WITHIN_TTL_WINDOW("rows", false),
//        DELETED_TTL_KEYS_IN_MULTIPLE_SEGMENTS("rows", false),
//        METADATA_TTL_PRIMARY_KEYS_REMOVED("rows", false),
//        UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT("segments", false),
////        UPSERT_PRELOAD_FAILURE("count", false),
//    UPSERT_KEYS_IN_WRONG_SEGMENT("rows", false),
//        PARTIAL_UPSERT_OUT_OF_ORDER("rows", false),
//        PARTIAL_UPSERT_KEYS_NOT_REPLACED("rows", false),
//        UPSERT_OUT_OF_ORDER("rows", false),
//        DELETED_KEYS_TTL_PRIMARY_KEYS_REMOVED("rows", false),
//        TOTAL_KEYS_MARKED_FOR_DELETION("rows", false),
//        DELETED_KEYS_WITHIN_TTL_WINDOW("rows", false),
//        DELETED_TTL_KEYS_IN_MULTIPLE_SEGMENTS("rows", false),
//        METADATA_TTL_PRIMARY_KEYS_REMOVED("rows", false),
//        UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT("segments", false),

    //raw table name
//    SEGMENT_UPLOAD_TIMEOUT("segments", false),
//    SEGMENT_UPLOAD_SUCCESS("segments", false),
//    SEGMENT_UPLOAD_FAILURE("segments", false),

//    //clientId
//    REALTIME_ROWS_FETCHED("rows", false),
//    REALTIME_ROWS_FILTERED("rows", false),
//    INVALID_REALTIME_ROWS_DROPPED("rows", false),
//    INCOMPLETE_REALTIME_ROWS_CONSUMED("rows", false),
//    //table stream name
//    REALTIME_CONSUMPTION_EXCEPTIONS("exceptions", true),
//    ROWS_WITH_ERRORS("rows", false),

  }

  @Test
  public void serverGaugeTest()
      throws Exception {

    //global gauges
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, 1);
    _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, 1L);
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.JVM_HEAP_USED_BYTES, 53L);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_DIRECT_MEMORY, () -> 54L);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_HEAP_MEMORY, () -> 55L);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_DIRECT, () -> 56L);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_HEAP, () -> 57L);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_SMALL, () -> 58L);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_NORMAL, () -> 59L);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CHUNK_SIZE, () -> 60L);

    //local gauges
    //that accept tableNameWithType
    String rawTableName = "myTable";
    String tableNameWithType = "myTable_REALTIME";

    _serverMetrics.addValueToTableGauge(tableNameWithType, ServerGauge.DOCUMENT_COUNT, 1L);
    _serverMetrics.addValueToTableGauge(tableNameWithType, ServerGauge.SEGMENT_COUNT, 5L);
    _serverMetrics.setValueOfPartitionGauge(tableNameWithType, 2, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, 50_000_000L);
    _serverMetrics.setValueOfPartitionGauge(tableNameWithType, 2, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT, 1000_000L);

    //raw table name
    _serverMetrics.addValueToTableGauge(rawTableName, ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, 40_000L);
    _serverMetrics.setValueOfTableGauge(rawTableName, ServerGauge.REALTIME_MERGED_TEXT_IDX_DOCUMENT_AVG_LEN, 50_000L);
    _serverMetrics.addValueToTableGauge(rawTableName, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, 60_000L);

    //that accept clientId
    String clientId = tableNameWithType + "-" + "myTopic" + "-" + "myPartitionGroupId" + "-" + "myClientId";

    _serverMetrics.setValueOfTableGauge(clientId, ServerGauge.LLC_PARTITION_CONSUMING, 1L);
    _serverMetrics.setValueOfTableGauge(clientId, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED, 54L);
    _serverMetrics.setValueOfTableGauge(clientId, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(76_000));
    _serverMetrics.setValueOfTableGauge(clientId, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(50_000));
    _serverMetrics.setValueOfTableGauge(clientId,
        ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(24_000));
    _serverMetrics.setValueOfTableGauge(clientId, ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(32_000));
    _serverMetrics.setValueOfTableGauge(clientId, ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(34_000));
    _serverMetrics.setValueOfTableGauge("", ServerGauge.CONSUMPTION_QUOTA_UTILIZATION, 40);

    //todo: handle metric key names
//    serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.CONSUMPTION_QUOTA_UTILIZATION,
//        ratioPercentage);

    String tableStreamName = tableNameWithType + "_" + "myTableStreamTopic";
    _serverMetrics.setValueOfTableGauge(tableStreamName, ServerGauge.STREAM_DATA_LOSS, 1L);

    _serverMetrics.setOrUpdatePartitionGauge(tableNameWithType, 43, ServerGauge.REALTIME_INGESTION_DELAY_MS,
        () -> 3600L);
    _serverMetrics.setOrUpdatePartitionGauge(tableNameWithType, 45, ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS,
        () -> 4300L);
    _serverMetrics.setOrUpdatePartitionGauge("myTable", 21, ServerGauge.LUCENE_INDEXING_DELAY_MS, () -> 21L);
    _serverMetrics.setOrUpdatePartitionGauge("myTable", 23, ServerGauge.LUCENE_INDEXING_DELAY_DOCS, () -> 50L);
    _serverMetrics.setValueOfPartitionGauge(tableNameWithType, 3, ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT,
        23000);
    _serverMetrics.setValueOfPartitionGauge(tableNameWithType, 3, ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT,
        30_000_000);
    _serverMetrics.setOrUpdatePartitionGauge(tableNameWithType, 4, ServerGauge.REALTIME_INGESTION_OFFSET_LAG,
        () -> 34L);

    HttpClient httpClient = new HttpClient();
    SimpleHttpResponse response = httpClient.sendGetRequest(new URI("http://localhost:9021/metrics"));

    List<PromMetric> exportedPrometheusMetrics = parseExportedPromMetrics(response.getResponse());

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_realtimeIngestionOffsetLag_Value",
            List.of("partition", "4", "table", "myTable", "tableType", "REALTIME"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_realtimeIngestionOffsetLag_Value",
            List.of("partition", "4", "table", "myTable", "tableType", "REALTIME"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_realtimeIngestionOffsetLag_Value",
            List.of("partition", "4", "table", "myTable", "tableType", "REALTIME"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_upsertPrimaryKeysInSnapshotCount_Value",
            List.of("partition", "3", "table", "myTable", "tableType", "REALTIME"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_upsertValidDocIdSnapshotCount_Value",
            List.of("partition", "3", "table", "myTable", "tableType", "REALTIME"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_endToEndRealtimeIngestionDelayMs_Value",
            List.of("partition", "45", "table", "myTable", "tableType", "REALTIME"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_realtimeIngestionDelayMs_Value",
            List.of("partition", "43", "table", "myTable", "tableType", "REALTIME"))));

    //we only match the metric name for pinot_server_version as the label value is dynamic (based on project version)
    Optional<PromMetric> pinotServerVersionMetricMaybe = exportedPrometheusMetrics.stream()
        .filter(exportedMetric -> exportedMetric._metricName.contains("pinot_server_version")).findAny();

    Assert.assertTrue(pinotServerVersionMetricMaybe.isPresent());

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_llcSimultaneousSegmentBuilds_Value")));

    Assert.assertTrue(exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_jvmHeapUsedBytes_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledUsedDirectMemory_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledUsedHeapMemory_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledUsedHeapMemory_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledArenasDirect_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledArenasHeap_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledArenasHeap_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledCacheSizeSmall_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledCacheSizeNormal_Value")));

    Assert.assertTrue(
        exportedPrometheusMetrics.contains(PromMetric.withName("pinot_server_nettyPooledChunkSize_Value")));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_llcPartitionConsuming_Value",
            List.of("partition", "myClientId", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_highestStreamOffsetConsumed_Value",
            List.of("partition", "myClientId", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_lastRealtimeSegmentCreationWaitTimeSeconds_Value",
            List.of("partition", "myClientId", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_lastRealtimeSegmentInitialConsumptionDurationSeconds_Value",
            List.of("partition", "myClientId", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_lastRealtimeSegmentCatchupDurationSeconds_Value",
            List.of("partition", "myClientId", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));
  }

  public void addGlobalMeter(ServerMeter serverMeter) {
    _serverMetrics.addMeteredGlobalValue(serverMeter, 4L);
  }

  public void addMeterWithLables(ServerMeter serverMeter, String label) {
    _serverMetrics.addMeteredTableValue(label, serverMeter, 4L);
  }

  private List<PromMetric> parseExportedPromMetrics(String response)
      throws IOException {

    List<PromMetric> exportedPromMetrics = new ArrayList<>();

    BufferedReader reader = new BufferedReader(new StringReader(response));

    String line;
    while ((line = reader.readLine()) != null) {
      if (line.startsWith("pinot_")) {
        exportedPromMetrics.add(PromMetric.fromExportedMetric(line));
      }
    }
    reader.close();
    return exportedPromMetrics;
  }

  public static class PromMetric {
    private final String _metricName;
    private final Map<String, String> _labels;

    private PromMetric(String metricName, Map<String, String> labels) {
      this._metricName = metricName;
      this._labels = labels;
    }

    public static PromMetric fromExportedMetric(String exportedMetric) {
      int spaceIndex = exportedMetric.indexOf(' ');
      String metricWithoutVal = exportedMetric.substring(0, spaceIndex);
      int braceIndex = metricWithoutVal.indexOf('{');

      if (braceIndex != -1) {
        String metricName = metricWithoutVal.substring(0, braceIndex);
        String labelsString = metricWithoutVal.substring(braceIndex + 1, metricWithoutVal.lastIndexOf('}'));
        Map<String, String> labels = parseLabels(labelsString);
        return new PromMetric(metricName, labels);
      } else {
        return new PromMetric(metricWithoutVal, new LinkedHashMap<>());
      }
    }

    private static Map<String, String> parseLabels(String labelsString) {
      return labelsString.isEmpty() ? new LinkedHashMap<>()
          : java.util.Arrays.stream(labelsString.split(",")).map(kvPair -> kvPair.split("="))
              .collect(Collectors.toMap(kv -> kv[0], kv -> removeQuotes(kv[1]), (v1, v2) -> v2, LinkedHashMap::new));
    }

    private static String removeQuotes(String value) {
      return value.startsWith("\"") ? value.substring(1, value.length() - 1) : value;
    }

    public static PromMetric withName(String metricName) {
      return new PromMetric(metricName, new LinkedHashMap<>());
    }

    public static PromMetric withNameAndLabels(String metricName, List<String> labels) {
      Map<String, String> labelMap = new LinkedHashMap<>();
      for (int i = 0; i < labels.size(); i += 2) {
        labelMap.put(labels.get(i), labels.get(i + 1));
      }
      return new PromMetric(metricName, labelMap);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PromMetric that = (PromMetric) o;
      return Objects.equal(_metricName, that._metricName) && Objects.equal(_labels, that._labels);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(_metricName, _labels);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(_metricName);
      if (!_labels.isEmpty()) {
        sb.append('{');
        sb.append(_labels.entrySet().stream().map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
            .collect(Collectors.joining(",")));
        sb.append('}');
      }
      return sb.toString();
    }
  }
}

