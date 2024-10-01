package org.apache.pinot.common.metrics;

import java.util.stream.IntStream;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.reporting.JmxReporter;
import org.apache.pinot.plugin.metrics.yammer.YammerMetricsRegistry;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


public class MetricsExposureTest {
  private ServerMetrics serverMetrics;
  private JmxReporter jmxReporter;


  @Test
  public void testMetricsExposure() throws Exception {

    PinotConfiguration pinotConfiguration = new PinotConfiguration();
    pinotConfiguration.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory");
    PinotMetricUtils.init(pinotConfiguration);

    // Initialize ServerMetrics with the registry
    YammerMetricsRegistry yammerMetricsRegistry = new YammerMetricsRegistry();
    ServerMetrics serverMetrics = new ServerMetrics(yammerMetricsRegistry);

    // Enable JMX reporting
    MetricsRegistry metricsRegistry = (MetricsRegistry) yammerMetricsRegistry.getMetricsRegistry();
    jmxReporter = new JmxReporter(metricsRegistry);
    jmxReporter.start();


    String metricName = "testMultipleUpdates";

    // update and remove gauge simultaneously
    IntStream.range(0, 1000).forEach(i -> {
      serverMetrics.setOrUpdateGauge(metricName, () -> (long) i);
    });


    System.out.println("Metrics are exposed. Press Enter to exit...");
    System.in.read();
  }
}

