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
import java.util.function.Supplier;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerMetricsTest {
  private ServerMetrics _serverMetrics;

  private HttpClient _httpClient;

  private static final String EXPORTED_METRIC_PREFIX = "pinot_server_";

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
    assertTimerExportedCorrectly("freshnessLagMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("nettyConnectionSendResponseLatency", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("executionThreadCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("systemActivitiesCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("responseSerCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("segmentUploadTimeMs", List.of("table", RAW_TABLE_NAME));

    assertTimerExportedCorrectly("totalCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("upsertPreloadTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("totalCpuTimeNs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("upsertRemoveExpiredPrimaryKeysTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("grpcQueryExecutionMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("upsertSnapshotTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("dedupRemoveExpiredPrimaryKeysTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);
    assertTimerExportedCorrectly("secondaryQWaitTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    //assert on all global timers
    assertTimerExportedCorrectly("hashJoinBuildTableCpuTimeMs");
    assertTimerExportedCorrectly("multiStageSerializationCpuTimeMs");
    assertTimerExportedCorrectly("multiStageDeserializationCpuTimeMs");
    assertTimerExportedCorrectly("receiveDownstreamWaitCpuTimeMs");
    assertTimerExportedCorrectly("receiveUpstreamWaitCpuTimeMs");

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
    assertMeterExportedCorrectly("queries");

    addGlobalMeter(ServerMeter.UNCAUGHT_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_uncaught");

    addGlobalMeter(ServerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_requestDeserialization");

    addGlobalMeter(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_responseSerialization");

    addGlobalMeter(ServerMeter.QUERY_EXECUTION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_exceptions_queryExecution");

    addGlobalMeter(ServerMeter.HELIX_ZOOKEEPER_RECONNECTS);
    assertMeterExportedCorrectly("helix_zookeeperReconnects");

    addGlobalMeter(ServerMeter.REALTIME_ROWS_CONSUMED);
    assertMeterExportedCorrectly("realtime_rowsConsumed");

    addGlobalMeter(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS);
    assertMeterExportedCorrectly("realtime_consumptionExceptions");

    addMeterWithLables(ServerMeter.REALTIME_ROWS_CONSUMED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsConsumed", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_SANITIZED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsSanitized", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_FETCHED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsFetched", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.REALTIME_ROWS_FILTERED, CLIENT_ID);
    assertMeterExportedCorrectly("realtimeRowsFiltered", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.INVALID_REALTIME_ROWS_DROPPED, CLIENT_ID);
    assertMeterExportedCorrectly("invalidRealtimeRowsDropped", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.INCOMPLETE_REALTIME_ROWS_CONSUMED, CLIENT_ID);
    assertMeterExportedCorrectly("incompleteRealtimeRowsConsumed", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.STREAM_CONSUMER_CREATE_EXCEPTIONS, CLIENT_ID);

    //todo: REALTIME_OFFSET_COMMITS, REALTIME_OFFSET_COMMIT_EXCEPTIONS

    addMeterWithLables(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, TABLE_STREAM_NAME);
    assertMeterExportedCorrectly("realtimeConsumptionExceptions", List.of("table", "myTable_REALTIME_myTopic"));

    addMeterWithLables(ServerMeter.REALTIME_MERGED_TEXT_IDX_TRUNCATED_DOCUMENT_SIZE, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("realtimeMergedTextIdxTruncatedDocumentSize", List.of("table", "myTable"));

    addMeterWithLables(ServerMeter.QUERIES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("queries", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SECONDARY_QUERIES_SCHEDULED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSecondaryQueriesScheduled", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SCHEDULING_TIMEOUT_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("schedulingTimeoutExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.QUERY_EXECUTION_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("queryExecutionExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_SEGMENT_COUNT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedSegmentCount", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETE_TABLE_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deleteTableFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.REALTIME_PARTITION_MISMATCH, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("realtimePartitionMismatch", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.REALTIME_DEDUP_DROPPED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("realtimeDedupDropped", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_KEYS_IN_WRONG_SEGMENT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertKeysInWrongSegment", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_OUT_OF_ORDER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertOutOfOrder", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("partialUpsertKeysNotReplaced", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("partialUpsertOutOfOrder", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_KEYS_TTL_PRIMARY_KEYS_REMOVED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedKeysTtlPrimaryKeysRemoved", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.TOTAL_KEYS_MARKED_FOR_DELETION, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("totalKeysMarkedForDeletion", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_KEYS_WITHIN_TTL_WINDOW, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedKeysWithinTtlWindow", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.DELETED_TTL_KEYS_IN_MULTIPLE_SEGMENTS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("deletedTtlKeysInMultipleSegments", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.METADATA_TTL_PRIMARY_KEYS_REMOVED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("metadataTtlPrimaryKeysRemoved", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_MISSED_VALID_DOC_ID_SNAPSHOT_COUNT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertMissedValidDocIdSnapshotCount", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UPSERT_PRELOAD_FAILURE, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("upsertPreloadFailure", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_DOCS_SCANNED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numDocsScanned", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_IN_FILTER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numEntriesScannedInFilter", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_ENTRIES_SCANNED_POST_FILTER, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numEntriesScannedPostFilter", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_QUERIED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsQueried", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PROCESSED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsProcessed", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_MATCHED, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsMatched", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_MISSING_SEGMENTS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numMissingSegments", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.RELOAD_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("reloadFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.REFRESH_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("refreshFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.UNTAR_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("untarFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_STREAMED_DOWNLOAD_UNTAR_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentStreamedDownloadUntarFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DIR_MOVEMENT_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDirMovementFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDownloadFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_REMOTE_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDownloadFromRemoteFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.SEGMENT_DOWNLOAD_FROM_PEERS_FAILURES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("segmentDownloadFromPeersFailures", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_RESIZES, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numResizes", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.RESIZE_TIME_MS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("resizeTimeMs", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_INVALID, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsPrunedInvalid", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_LIMIT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsPrunedByLimit", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.NUM_SEGMENTS_PRUNED_BY_VALUE, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("numSegmentsPrunedByValue", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSES_SENT, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("largeQueryResponsesSent", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.TOTAL_THREAD_CPU_TIME_MILLIS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("totalThreadCpuTimeMillis", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.LARGE_QUERY_RESPONSE_SIZE_EXCEPTIONS, TABLE_NAME_WITH_TYPE);
    assertMeterExportedCorrectly("largeQueryResponseSizeExceptions", EXPORTED_LABELS_FOR_TABLE_NAME_TABLE_TYPE);

    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, CLIENT_ID);
    assertMeterExportedCorrectly("rowsWithErrors", EXPORTED_LABELS_FOR_CLIENT_ID);

    addMeterWithLables(ServerMeter.ROWS_WITH_ERRORS, TABLE_STREAM_NAME);
    assertMeterExportedCorrectly("rowsWithErrors", List.of("table", "myTable_REALTIME_myTopic"));

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_FAILURE, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("segmentUploadFailure", List.of("table", RAW_TABLE_NAME));

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_SUCCESS, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("segmentUploadSuccess", List.of("table", RAW_TABLE_NAME));

    addMeterWithLables(ServerMeter.SEGMENT_UPLOAD_TIMEOUT, RAW_TABLE_NAME);
    assertMeterExportedCorrectly("segmentUploadTimeout", List.of("table", RAW_TABLE_NAME));
  }

  @Test
  public void serverGaugeTest()
      throws Exception {

    //get all exposed metrics before we expose any timers
    List<PromMetric> promMetricsBefore = parseExportedPromMetrics(getExportedPromMetrics().getResponse());

    int partition = 3;
    long someVal = 100L;
    Supplier<Long> someValSupplier = () -> someVal;

    //global gauges
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.VERSION, PinotVersion.VERSION_METRIC_NAME, someVal);
    _serverMetrics.addValueToGlobalGauge(ServerGauge.LLC_SIMULTANEOUS_SEGMENT_BUILDS, someVal);
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.JVM_HEAP_USED_BYTES, someVal);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_DIRECT_MEMORY, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_USED_HEAP_MEMORY, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_DIRECT, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_ARENAS_HEAP, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_SMALL, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CACHE_SIZE_NORMAL, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_CHUNK_SIZE, someValSupplier);
    _serverMetrics.setOrUpdateGlobalGauge(ServerGauge.NETTY_POOLED_THREADLOCALCACHE, someValSupplier);

    //local gauges
    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.DOCUMENT_COUNT, someVal);
    _serverMetrics.addValueToTableGauge(TABLE_NAME_WITH_TYPE, ServerGauge.SEGMENT_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, 2, ServerGauge.DEDUP_PRIMARY_KEYS_COUNT, someVal);

    //raw table name
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_OFFHEAP_MEMORY_USED, someVal);
    _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_MERGED_TEXT_IDX_DOCUMENT_AVG_LEN, someVal);
    _serverMetrics.addValueToTableGauge(RAW_TABLE_NAME, ServerGauge.REALTIME_SEGMENT_NUM_PARTITIONS, someVal);

    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LLC_PARTITION_CONSUMING, someVal);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.HIGHEST_STREAM_OFFSET_CONSUMED, someVal);
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CREATION_WAIT_TIME_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID,
        ServerGauge.LAST_REALTIME_SEGMENT_INITIAL_CONSUMPTION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_CATCHUP_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(CLIENT_ID, ServerGauge.LAST_REALTIME_SEGMENT_COMPLETION_DURATION_SECONDS,
        TimeUnit.MILLISECONDS.toSeconds(someVal));
    _serverMetrics.setValueOfTableGauge(RAW_TABLE_NAME, ServerGauge.CONSUMPTION_QUOTA_UTILIZATION, someVal);

    _serverMetrics.setValueOfTableGauge(TABLE_STREAM_NAME, ServerGauge.STREAM_DATA_LOSS, 1L);

    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.REALTIME_INGESTION_DELAY_MS,
        someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.END_TO_END_REALTIME_INGESTION_DELAY_MS, someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(RAW_TABLE_NAME, partition, ServerGauge.LUCENE_INDEXING_DELAY_MS,
        someValSupplier);
    _serverMetrics.setOrUpdatePartitionGauge(RAW_TABLE_NAME, partition, ServerGauge.LUCENE_INDEXING_DELAY_DOCS,
        someValSupplier);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, someVal);
    _serverMetrics.setValueOfPartitionGauge(TABLE_NAME_WITH_TYPE, partition,
        ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT, someVal);
    _serverMetrics.setOrUpdatePartitionGauge(TABLE_NAME_WITH_TYPE, partition, ServerGauge.REALTIME_INGESTION_OFFSET_LAG,
        someValSupplier);

    List<String> labels = List.of("partition", String.valueOf(partition), "table", RAW_TABLE_NAME, "tableType",
        TableType.REALTIME.toString());

    assertGaugeExportedCorrectly("realtimeIngestionOffsetLag", labels);

    assertGaugeExportedCorrectly("upsertPrimaryKeysInSnapshotCount", labels);

    assertGaugeExportedCorrectly("upsertValidDocIdSnapshotCount", labels);

    assertGaugeExportedCorrectly("endToEndRealtimeIngestionDelayMs", labels);

    assertGaugeExportedCorrectly("realtimeIngestionDelayMs", labels);

//    we only match the metric name for version as the label value is dynamic (based on project version).
    Optional<PromMetric> pinotServerVersionMetricMaybe =
        parseExportedPromMetrics(getExportedPromMetrics().getResponse()).stream()
            .filter(exportedMetric -> exportedMetric._metricName.contains("version")).findAny();

    Assert.assertTrue(pinotServerVersionMetricMaybe.isPresent());

    assertGaugeExportedCorrectly("llcSimultaneousSegmentBuilds");
    assertGaugeExportedCorrectly("nettyPooledUsedDirectMemory");
    assertGaugeExportedCorrectly("nettyPooledUsedHeapMemory");
    assertGaugeExportedCorrectly("nettyPooledUsedHeapMemory");
    assertGaugeExportedCorrectly("nettyPooledArenasDirect");
    assertGaugeExportedCorrectly("nettyPooledArenasHeap");
    assertGaugeExportedCorrectly("nettyPooledCacheSizeSmall");
    assertGaugeExportedCorrectly("nettyPooledCacheSizeNormal");
    assertGaugeExportedCorrectly("nettyPooledChunkSize");
    assertGaugeExportedCorrectly("jvmHeapUsedBytes");

    assertGaugeExportedCorrectly("llcPartitionConsuming", EXPORTED_LABELS_FOR_CLIENT_ID);

    assertGaugeExportedCorrectly("highestStreamOffsetConsumed", EXPORTED_LABELS_FOR_CLIENT_ID);

    assertGaugeExportedCorrectly("lastRealtimeSegmentCreationWaitTimeSeconds", EXPORTED_LABELS_FOR_CLIENT_ID);

    assertGaugeExportedCorrectly("lastRealtimeSegmentInitialConsumptionDurationSeconds", EXPORTED_LABELS_FOR_CLIENT_ID);

    assertGaugeExportedCorrectly("lastRealtimeSegmentCatchupDurationSeconds", EXPORTED_LABELS_FOR_CLIENT_ID);

    List<PromMetric> promMetricsAfter = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertEquals(promMetricsAfter.size() - promMetricsBefore.size(), ServerGauge.values().length);
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

  private void assertGaugeExportedCorrectly(String exportedGaugePrefix)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertTrue(
        promMetrics.contains(PromMetric.withName(EXPORTED_METRIC_PREFIX + exportedGaugePrefix + "_" + "Value")));
  }

  private void assertGaugeExportedCorrectly(String exportedGaugePrefix, List<String> labels)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    Assert.assertTrue(promMetrics.contains(
        PromMetric.withNameAndLabels(EXPORTED_METRIC_PREFIX + exportedGaugePrefix + "_" + "Value", labels)));
  }

  private void assertTimerExportedCorrectly(String exportedTimerPrefix)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : TIMER_TYPES) {
      Assert.assertTrue(
          promMetrics.contains(PromMetric.withName(EXPORTED_METRIC_PREFIX + exportedTimerPrefix + "_" + meterType)));
    }
  }

  private void assertTimerExportedCorrectly(String exportedTimerPrefix, List<String> labels)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(promMetrics.contains(
          PromMetric.withNameAndLabels(EXPORTED_METRIC_PREFIX + exportedTimerPrefix + "_" + meterType, labels)));
    }
  }

  private void assertMeterExportedCorrectly(String exportedMeterPrefix)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(
          promMetrics.contains(PromMetric.withName(EXPORTED_METRIC_PREFIX + exportedMeterPrefix + "_" + meterType)));
    }
  }

  private void assertMeterExportedCorrectly(String exportedMeterPrefix, List<String> labels)
      throws IOException, URISyntaxException {
    List<PromMetric> promMetrics = parseExportedPromMetrics(getExportedPromMetrics().getResponse());
    for (String meterType : METER_TYPES) {
      Assert.assertTrue(promMetrics.contains(
          PromMetric.withNameAndLabels(EXPORTED_METRIC_PREFIX + exportedMeterPrefix + "_" + meterType, labels)));
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

