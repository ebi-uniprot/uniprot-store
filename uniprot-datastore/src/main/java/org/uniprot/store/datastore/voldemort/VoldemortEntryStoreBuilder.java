package org.uniprot.store.datastore.voldemort;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * This class contains common methods that are used to build Voldemort data and also to provide statistics
 *
 * Created 19/04/2016
 *
 * @author wudong
 */
public abstract class VoldemortEntryStoreBuilder<T> {

    private static final Logger logger = LoggerFactory.getLogger(VoldemortEntryStoreBuilder.class);

    protected final VoldemortClient<T> remoteStore;
    protected final Slf4jReporter reporter;

    protected final Timer string_splitter_time;
    protected final Timer entry_store_time;
    protected final Timer entry_parse_convert_time;
    protected final Counter counter_total;
    protected final Counter counter_parse_success;
    protected final Counter counter_parse_fail;
    protected final Counter counter_store_success;
    protected final Counter counter_store_fail;
    protected final Counter counter_try;

    //toRetry while having failed stroed entrie.
    protected final AtomicBoolean hasStoreFailure = new AtomicBoolean();
    protected final AtomicBoolean hasParseFailure = new AtomicBoolean();
    private String metricsPrefix;

    public VoldemortEntryStoreBuilder(VoldemortClient<T> remoteStore, String metricsPrefix) throws IOException {
        this.remoteStore = remoteStore;

        Slf4jReporter reporter = Slf4jReporter.forRegistry(MetricsUtil.getMetricRegistryInstance()).build();
        reporter.start(10, TimeUnit.MINUTES);
        this.reporter = reporter;
        this.metricsPrefix = metricsPrefix;
        
        this.string_splitter_time = MetricsUtil.getMetricRegistryInstance().timer(metricsPrefix+"-entry-string-splitter-time");
        this.counter_total = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-total");
        this.counter_parse_success = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-parse-success");
        this.counter_parse_fail = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-parse-fail");
        this.counter_store_success = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-store-success");
        this.counter_store_fail = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-entry-store-fail");
        this.entry_store_time = MetricsUtil.getMetricRegistryInstance().timer(metricsPrefix+"-entry-store-time");
        this.entry_parse_convert_time = MetricsUtil.getMetricRegistryInstance().timer(metricsPrefix+"-entry-parse-convert-time");
        this.counter_try = MetricsUtil.getMetricRegistryInstance().counter(metricsPrefix+"-store-try");
    }

    public void retryBuild(String ff) {
        int itr = 0;
        String parseFail = metricsPrefix+".parse.fail.";
        String storingFail = metricsPrefix+".storing.fail.";

        int retryCounter = 0;

        do {
            counter_try.inc();
            //reset the indicator to retry.
            hasStoreFailure.set(false);
            String fileToLoad = retryCounter == 0 ? ff : storingFail + (retryCounter - 1) + ".txt";
            try {
                build(fileToLoad, parseFail + (retryCounter) + ".txt", storingFail + (retryCounter) + ".txt", retryCounter > 0);
            } catch (IOException e) {
                logger.error("Unable to run build with IO Exception", e);
                //exit the building process.
                return;
            }
            retryCounter++;
        } while (hasStoreFailure.get() && retryCounter < 10);

        if (hasStoreFailure.get()) {
            logger.warn("Building failure from Storing entries still exist, please check last Stroing failed file");
        } else {
            if (hasParseFailure.get()) {
                logger.warn("Building failure from Parsing entries exist, please check last Parsing failed file");
            } else {
                logger.info("Build success after {} number of build iteration", retryCounter);
            }
        }
    }

    protected abstract void build(String ff, String parseFailFile, String storingFailFile, boolean retry) throws IOException;

    public Map<String, Counter> getExecutionStatistics(){
        return MetricsUtil.getMetricRegistryInstance().getCounters();
    }

    protected void generateStatsFile(String outputFileName, String releaseNumber) {
        SortedMap<String, Counter> counters = MetricsUtil.getMetricRegistryInstance().getCounters();
        Map<String, String> stats = counters.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> String.valueOf(e.getValue().getCount())));

        Properties properties = new Properties();
        properties.putAll(stats);
        properties.put("uniprot_release", releaseNumber);

        try (FileOutputStream fileOutputStream = new FileOutputStream(outputFileName)) {
            properties.store(fileOutputStream, null);
        } catch (IOException e) {
            logger.warn("Error while writing InfoObject file on path: " + outputFileName, e);
        }
    }

    static public class LimitedQueue<E> extends LinkedBlockingQueue<E> {
        public LimitedQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        public boolean offer(E e) {
            // turn offer() and add() into a blocking calls (unless interrupted)
            try {
                put(e);
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
