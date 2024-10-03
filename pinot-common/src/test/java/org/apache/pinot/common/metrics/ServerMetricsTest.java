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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerMetricsTest {
  private ServerMetrics _serverMetrics;

  private HttpClient _httpClient;

  private static final List<String> METER_TYPES =
      List.of("Count", "FiveMinuteRate", "MeanRate", "OneMinuteRate", "FifteenMinuteRate");

  private static final List<String> TIMER_TYPES =
      List.of("Count", "FiveMinuteRate", "Max", "999thPercentile", "95thPercentile", "75thPercentile", "98thPercentile",
          "OneMinuteRate", "50thPercentile", "99thPercentile", "FifteenMinuteRate", "Mean", "StdDev", "MeanRate",
          "Min");

  private static final String RAW_TABLE_NAME = "myTable";
  private static final String TABLE_NAME_WITH_TYPE =
      TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(RAW_TABLE_NAME);

  private static final String KAFKA_TOPIC = "myTopic";
  private static final String PARTITION_GROUP_ID = "partitionGroupId";
  private static final String CLIENT_ID =
      String.format("%s-%s-%s", TABLE_NAME_WITH_TYPE, KAFKA_TOPIC, PARTITION_GROUP_ID);
  private static final String TABLE_STREAM_NAME = String.format("%s_%s", TABLE_NAME_WITH_TYPE, KAFKA_TOPIC);

  private static final List<String> EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE =
      List.of("table", "myTable", "tableType", "REALTIME");

  private static final List<String> EXPORTED_LABELS_FOR_CLIENT_ID =
      List.of("partition", PARTITION_GROUP_ID, "table", RAW_TABLE_NAME, "tableType", TableType.REALTIME.toString(),
          "topic", KAFKA_TOPIC);

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

  /**
   * This test validates each timer defined in {@link ServerTimer}
   */
  @Test
  public void serverTimerTest()
      throws IOException, URISyntaxException {

    //get all exposed metrics before we expose any timers
    List<PromMetric> promMetricsBefore = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    for (ServerTimer serverTimer : ServerTimer.values()) {
      if (serverTimer.isGlobal()) {
        _serverMetrics.addTimedValue(serverTimer, 30_000, TimeUnit.MILLISECONDS);
      } else if (serverTimer == ServerTimer.SEGMENT_UPLOAD_TIME_MS) {
        _serverMetrics.addTimedTableValue(RAW_TABLE_NAME, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      } else {
        _serverMetrics.addTimedTableValue(TABLE_NAME_WITH_TYPE, serverTimer, 30_000L, TimeUnit.MILLISECONDS);
      }
    }
    //assert on timers with labels
    assertTimerExportedCorrectly("pinot_server_freshnessLagMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_nettyConnectionSendResponseLatency",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_executionThreadCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_systemActivitiesCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_responseSerCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_segmentUploadTimeMs", List.of("table", RAW_TABLE_NAME));

    assertTimerExportedCorrectly("pinot_server_totalCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_upsertPreloadTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_totalCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_upsertRemoveExpiredPrimaryKeysTimeMs",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_grpcQueryExecutionMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_upsertSnapshotTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_dedupRemoveExpiredPrimaryKeysTimeMs",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("pinot_server_secondaryQWaitTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    //assert on all global timers
    assertTimerExportedCorrectly("pinot_server_hashJoinBuildTableCpuTimeMs");
    assertTimerExportedCorrectly("pinot_server_multiStageSerializationCpuTimeMs");
    assertTimerExportedCorrectly("pinot_server_multiStageDeserializationCpuTimeMs");
    assertTimerExportedCorrectly("pinot_server_receiveDownstreamWaitCpuTimeMs");
    assertTimerExportedCorrectly("pinot_server_receiveUpstreamWaitCpuTimeMs");

    //now assert that we've added exported all timers present in ServerTimer.java
    List<PromMetric> promMetricsAfter = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertEquals(promMetricsAfter.size() - promMetricsBefore.size(),
        ServerTimer.values().length * TIMER_TYPES.size());
  }

  /**
   * This test validates each meter defined in {@link ServerMeter}
   */
  @Test
  public void serverMeterTest()
      throws Exception {

    //todo: Add test for NUM_SECONDARY_QUERIES as it's not used anywhere currently

    //todo: Add test for SERVER_OUT_OF_CAPACITY_EXCEPTIONS as it's not used anywhere currently

    addGlobalMeter(ServerMeter.QUERIES);
    assertMeterExportedCorrectly("pinot_server_queries");

    addGlobalMeter(ServerMeter.UNCAUGHT_EXCEPTIONS);
    assertMeterExportedCorrectly("pinot_server_realtime_exceptions_uncaught");

    addGlobalMeter(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS);
    assertMeterExportedCorrectly("pinot_server_realtime_exceptions_requestDeserialization");

    addGlobalMeter(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS);
    assertMeterExportedCorrectly("pinot_server_realtime_exceptions_responseSerialization");

    addGlobalMeter(ServerMeter.QUERY_EXECUTION_EXCEPTIONS);
    assertMeterExportedCorrectly("pinot_server_realtime_exceptions_queryExecution");

    addGlobalMeter(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS);
    assertMeterExportedCorrectly("pinot_server_helix_zookeeperReconnects");

    addGlobalMeter(ServerMeter.REALTIME_ROWS_CONSUMED);
    assertMeterExportedCorrectly("pinot_server_realtime_rowsConsumed");

    addGlobalMeter(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS);
    assertMeterExportedCorrectly("pinot_server_realtime_consumptionExceptions");

    addMeterWithLables(ServerMeter.REALTIME_ROWS_CONSUMED, CLIENT_ID);
    assertMeterExportedCorrectly("pinot_server_realtimeRowsConsumed", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_SANITIZED, CLIENT_ID);
    assertMeterExportedCorrectly("pinot_server_realtimeRowsSanitized", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_FETCHED, CLIENT_ID);
    assertMeterExportedCorrectly("pinot_server_realtimeRowsFetched", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_FILTERED, CLIENT_ID);
    assertMeterExportedCorrectly("pinot_server_realtimeRowsFiltered", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.INVALID_REALTIME_ROWS_DROPPED, CLIENT_ID);
    assertMeterExportedCorrectly("pinot_server_invalidRealtimeRowsDropped", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED, CLIENT_ID);
    assertMeterExportedCorrectly("pinot_server_incompleteRealtimeRowsConsumed", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, CLIENT_ID);

    //todo: REALTIME_OFFSET_COMMITS, REALTIME_OFFSET_COMMIT_EXCEPTIONS

    addMeterWithLables(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, TABLE_STREAM_NAME);
    assertMeterExportedCorrectly("pinot_server_realtimeConsumptionExceptions",
        List.of("table", "myTable_REALTIME_myTopic"));

    addMeterWithLables(ServerMeter.REALTIME_MERGED_TEXT_IDX_TRUNCATED_DOCUMENT_SIZE, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("pinot_server_realtimeMergedTextIdxTruncatedDocumentSize",
        List.of("table", "myTable"));

    addMeterWithLables(ServerMeter.QUERIES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_queries", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SECONDARY_QUERIES_SCHEDULED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numSecondaryQueriesScheduled",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_schedulingTimeoutExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.QUERY_EXECUTION_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_queryExecutionExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_SEGMENT_COUNT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_deletedSegmentCount", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETE_TABLE_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_deleteTableFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.REALTIME_PARTITION_MISMATCH, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_realtimePartitionMismatch", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.REALTIME_DEDUP_DROPPED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_realtimeDedupDropped", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_KEYS_IN_WRONG_SEGMENT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_upsertKeysInWrongSegment", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_OUT_OF_ORDER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_upsertOutOfOrder", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_partialUpsertKeysNotReplaced",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_partialUpsertOutOfOrder", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_KEYS_TTL_PRIMARY_KEYS_REMOVED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_deletedKeysTtlPrimaryKeysRemoved",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.TOTAL_KEYS_MARKED_FOR_DELETION, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_totalKeysMarkedForDeletion", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_KEYS_WITHIN_TTL_WINDOW, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_deletedKeysWithinTtlWindow", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_TTL_KEYS_IN_MULTIPLE_SEGMENTS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_deletedTtlKeysInMultipleSegments",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.METADATA_TTL_PRIMARY_KEYS_REMOVED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_metadataTtlPrimaryKeysRemoved",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_upsertMissedValidDocIdSnapshotCount",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_PRELOAD_FAILURE, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_upsertPreloadFailure", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_DOCS_SCANNED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numDocsScanned", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_IN_FILTER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numEntriesScannedInFilter", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_POST_FILTER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numEntriesScannedPostFilter", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_QUERIED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numSegmentsQueried", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PROCESSED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numSegmentsProcessed", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_MATCHED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numSegmentsMatched", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_MISSING_SEGMENTS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numMissingSegments", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.RELOAD_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_reloadFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.REFRESH_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_refreshFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UNTAR_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_untarFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_STREAMED_DOWNLOAD_UNTAR_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_segmentStreamedDownloadUntarFailures",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DIR_MOVEMENT_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_segmentDirMovementFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_segmentDownloadFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_REMOTE_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_segmentDownloadFromRemoteFailures",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_PEERS_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_segmentDownloadFromPeersFailures",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_RESIZES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numResizes", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.RESIZE_TIME_MS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_resizeTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_INVALID, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numSegmentsPrunedInvalid", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_LIMIT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numSegmentsPrunedByLimit", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_VALUE, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_numSegmentsPrunedByValue", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSES_SENT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_largeQueryResponsesSent", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.TOTAL_THREAD_CPU_TIME_MILLIS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_totalThreadCpuTimeMillis", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("pinot_server_largeQueryResponseSizeExceptions",
        EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, CLIENT_ID);
    assertMeterExportedCorrectly("pinot_server_rowsWithErrors", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, TABLE_STREAM_NAME);
    assertMeterExportedCorrectly("pinot_server_rowsWithErrors", List.of("table", "myTable_REALTIME_myTopic"));

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_FAILURE, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("pinot_server_segmentUploadFailure", List.of("table", RAW_TABLE_NAME));

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_SUCCESS, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("pinot_server_segmentUploadSuccess", List.of("table", RAW_TABLE_NAME));

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_TIMEOUT, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("pinot_server_segmentUploadTimeout", List.of("table", RAW_TABLE_NAME));
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
    //that accept TABLE_NAME_WITH_TYPE
    String RAW_TABLE_NAME = "myTable";
    String TABLE_NAME_WITH_TYPE = "myTable_REALTIME";

    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.DOCUMENT_COUNT, 1L);
    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.SEGMENT_COUNT, 5L);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        50_000_000L);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT, 1000_000L);

    //raw table name
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, 40_000L);
    _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_MERGED_TEXT_IDX_DOCUMENT_AVG_LEN, 50_000L);
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, 60_000L);

    //that accept CLIENT_ID
    String CLIENT_ID = TABLE_NAME_WITH_TYPE + "-" + "myTopic" + "-" + "myPartitionGroupId" + "-" + "myCLIENT_ID";

    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LLC_PARTITION_CONSUMING, 1L);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED, 54L);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(76_000));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(50_000));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID,
        ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(24_000));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(32_000));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(34_000));
    _serverMetrics.setValueOfTableGauge("", ServerGauge.CONSUMPTION_QUOTA_UTILIZATION, 40);

    //todo: handle metric key names
//    serverMetrics.setValueOfTableGauge(_metricKeyName, ServerGauge.CONSUMPTION_QUOTA_UTILIZATION,
//        ratioPercentage);

    String TABLE_STREAM_NAME = TABLE_NAME_WITH_TYPE + "_" + "myTableStreamTopic";
    _serverMetrics.setValueOfTableGauge(TABLE_STREAM_NAME, ServerGauge.STREAM_DATA_LOSS, 1L);

    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, 43, ServerGauge.REALTIME_INGESTION_DELAY_MS,
        () -> 3600L);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, 45,
        ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS, () -> 4300L);
    _serverMetrics.setOrUpdatePartitionGauge("myTable", 21, ServerGauge.LUCENE_INDEXING_DELAY_MS, () -> 21L);
    _serverMetrics.setOrUpdatePartitionGauge("myTable", 23, ServerGauge.LUCENE_INDEXING_DELAY_DOCS, () -> 50L);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 3, ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT,
        23000);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 3, ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT,
        30_000_000);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, 4, ServerGauge.REALTIME_INGESTION_OFFSET_LAG,
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
            List.of("partition", "myCLIENT_ID", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_highestStreamOffsetConsumed_Value",
            List.of("partition", "myCLIENT_ID", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_lastRealtimeSegmentCreationWaitTimeSeconds_Value",
            List.of("partition", "myCLIENT_ID", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_lastRealtimeSegmentInitialConsumptionDurationSeconds_Value",
            List.of("partition", "myCLIENT_ID", "table", "myTable", "tableType", "REALTIME", "topic",
                "myTopic-myPartitionGroupId"))));

    Assert.assertTrue(exportedPrometheusMetrics.contains(
        PromMetric.withNameAndLabels("pinot_server_lastRealtimeSegmentCatchupDurationSeconds_Value",
            List.of("partition", "myCLIENT_ID", "table", "myTable", "tableType", "REALTIME", "topic",
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

  private void assertTimerExportedCorrectly(String exportedTimerPrefix)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : TIMER_TYPES) {
      Assert.assertTrue(promMetrics.contains(PromMetric.withName(exportedTimerPrefix + "_" + meterType)));
    }
  }

  private void assertTimerExportedCorrectly(String exportedTimerPrefix, List<String> labels)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(
          promMetrics.contains(PromMetric.withNameAndLabels(exportedTimerPrefix + "_" + meterType, labels)));
    }
  }

  private void assertMeterExportedCorrectly(String exportedMeterPrefix)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(promMetrics.contains(PromMetric.withName(exportedMeterPrefix + "_" + meterType)));
    }
  }

  private void assertMeterExportedCorrectly(String exportedMeterPrefix, List<String> labels)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(
          promMetrics.contains(PromMetric.withNameAndLabels(exportedMeterPrefix + "_" + meterType, labels)));
    }
  }

  private SimpleHttpResponse getExportedPromMetrics()
      throws IOException, URISyntaxException {
    return _httpClient.sendGetRequest(new URI("http://localhost:9021/metrics"));
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

