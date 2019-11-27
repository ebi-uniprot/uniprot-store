package org.uniprot.store.datastore.voldemort;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

/**
 * Created 24/05/2016
 *
 * @author wudong
 */
public class MetricsUtil {
    public final MetricRegistry metrics = new MetricRegistry();

    private static final MetricsUtil instance = new MetricsUtil();

    public MetricsUtil() {
        final JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
    }

    public static MetricRegistry getMetricRegistryInstance() {
        return instance.metrics;
    }

    public static void resetMetrics() {
        instance.metrics
                .getCounters()
                .values()
                .forEach(
                        counter -> {
                            counter.dec(counter.getCount());
                        });
    }
}
