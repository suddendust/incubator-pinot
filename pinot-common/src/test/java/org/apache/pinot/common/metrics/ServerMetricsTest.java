package org.apache.pinot.common.metrics;

import com.google.common.base.Objects;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class ServerMetricsTest {
  private ServerMetrics _serverMetrics;

  private HttpClient _httpClient;
  ;

  private static final List<ServerGauge> EXCLUDED_GAUGES =
      List.of(ServerGauge.CONSUMPTION_QUOTA_UTILIZATION, ServerGauge.LUCENE_INDEXING_DELAY_MS,
          ServerGauge.LUCENE_INDEXING_DELAY_DOCS);

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

  //  @Test
  public void serverMeterTest() {
    //validate all global metrics
    for (ServerMeter serverMeter : ServerMeter.values()) {
      if (serverMeter.isGlobal()) {
        _serverMetrics.addMeteredGlobalValue(serverMeter, 5L);
        try {
          SimpleHttpResponse response = _httpClient.sendGetRequest(new URI("http://localhost:9021/metrics"));
          List<PromMetric> promMetrics = parseExportedPromMetrics(response.getResponse());

          //for global meters (like `pinot_server_queries_OneMinuteRate`), we only want to match the prefix
          Optional<Boolean> exportedMetricMaybe = promMetrics.stream()
              .map(promMetric -> promMetric._metricName.contains("pinot_server_" + serverMeter.getMeterName()))
              .findFirst();

          Assert.assertTrue(exportedMetricMaybe.get(), "ServerMeter: " + serverMeter.getMeterName());
        } catch (Exception e) {

        }
      }
    }
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

