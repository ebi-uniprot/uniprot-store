package org.uniprot.store.datastore.voldemort.data.performance;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 * This class is responsible for testing directly the performance of a Voldemort client to
 * uniprotkb, uniref, uniparc. Gatling stress testing tool cannot be used, unlike with REST
 * applications, because the connection to Voldemort is TCP.
 *
 * <p>Created 12/06/2020
 *
 * @author Edd
 */
@Slf4j
public class PerformanceChecker {
    static final List<String> PROPERTY_KEYS =
            asList(
                    "sleepDurationBeforeRequest",
                    "reportSizeIfGreaterThanBytes",
                    "logInterval",
                    "reportSlowFetchTimeout",
                    "storeFetchRetryDelayMillis",
                    "storeFetchMaxRetries",
                    "corePoolSize",
                    "maxPoolSize",
                    "keepAliveTime",
                    "storesCSV",
                    "filePath");

    static String propertiesFile;

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            log.error("Please supply properties file as single program argument");
            System.exit(1);
        } else {
            propertiesFile = args[0];
        }

        Config config = new Config();
        PerformanceChecker checker = new PerformanceChecker();

        // initialise properties
        final Properties properties = new Properties();
        try (InputStream stream = new FileInputStream(propertiesFile)) {
            properties.load(stream);
            checker.init(properties, config);
        } catch (IOException e) {
            log.error("Problem loading " + propertiesFile, e);
            System.exit(1);
        }

        // do requests
        RequestDispatcher dispatcher = new RequestDispatcher(config);
        dispatcher.run();

        config.getExecutorService().shutdown();
        config.getExecutorService().awaitTermination(2, TimeUnit.DAYS);

        // show statistics
        dispatcher.printStatisticsSummary();
    }

    Map<String, VoldemortClient<?>> createClientMap(Properties properties) {
        Map<String, VoldemortClient<?>> map = new HashMap<>();

        String uniProtKBStoreName = properties.getProperty("store.uniprotkb.storeName");
        if (uniProtKBStoreName != null) {
            map.put(
                    uniProtKBStoreName,
                    createUniProtKBStore(
                            Integer.parseInt(
                                    properties.getProperty("store.uniprotkb.numberOfConnections")),
                            uniProtKBStoreName,
                            properties.getProperty("store.uniprotkb.host")));
            log.info("Created UniProtKB Voldemort client");
        }

        String uniRefStoreName = properties.getProperty("store.uniref.storeName");
        if (uniRefStoreName != null) {
            map.put(
                    uniRefStoreName,
                    createUniRefStore(
                            Integer.parseInt(
                                    properties.getProperty("store.uniref.numberOfConnections")),
                            uniRefStoreName,
                            properties.getProperty("store.uniref.host")));
            log.info("Created UniRef Voldemort client");
        }

        String uniParcStoreName = properties.getProperty("store.uniparc.storeName");
        if (uniParcStoreName != null) {
            map.put(
                    uniParcStoreName,
                    createUniParcStore(
                            Integer.parseInt(
                                    properties.getProperty("store.uniparc.numberOfConnections")),
                            uniParcStoreName,
                            properties.getProperty("store.uniparc.host")));
            log.info("Created UniParc Voldemort client");
        }

        if (map.isEmpty()) {
            throw new IllegalStateException("No Voldemort clients defined");
        }

        return map;
    }

    VoldemortClient<?> createUniProtKBStore(int connections, String storeName, String host) {
        return new VoldemortRemoteUniProtKBEntryStore(connections, storeName, host);
    }

    VoldemortClient<?> createUniRefStore(int connections, String storeName, String host) {
        return new VoldemortRemoteUniRefEntryStore(connections, storeName, host);
    }

    VoldemortClient<?> createUniParcStore(int connections, String storeName, String host) {
        return new VoldemortRemoteUniParcEntryStore(connections, storeName, host);
    }

    private ThreadPoolExecutor createExecutor(Properties properties) {
        return new ThreadPoolExecutor(
                Integer.parseInt(properties.getProperty("corePoolSize")),
                Integer.parseInt(properties.getProperty("maxPoolSize")),
                Integer.parseInt(properties.getProperty("keepAliveTime")),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
    }

    void init(Properties properties, Config config) throws FileNotFoundException {
        PROPERTY_KEYS.forEach(
                key -> {
                    if (!properties.containsKey(key)
                            && !Objects.nonNull(properties.getProperty(key))) {
                        throw new IllegalArgumentException(
                                "Must supply the '" + key + "' property");
                    }
                });

        config.setSleepDurationBeforeRequest(
                Integer.parseInt(properties.getProperty("sleepDurationBeforeRequest")));
        config.setReportSizeIfGreaterThanBytes(
                Integer.parseInt(properties.getProperty("reportSizeIfGreaterThanBytes")));

        config.setReportSlowFetchTimeout(
                Integer.parseInt(properties.getProperty("reportSlowFetchTimeout")));

        config.setLogInterval(Integer.parseInt(properties.getProperty("logInterval")));

        config.setRetryPolicy(
                new RetryPolicy<>()
                        .handle(IOException.class)
                        .withDelay(
                                Duration.ofMillis(
                                        Long.parseLong(
                                                properties.getProperty(
                                                        "storeFetchRetryDelayMillis"))))
                        .withMaxRetries(
                                Integer.parseInt(properties.getProperty("storeFetchMaxRetries"))));

        config.setStores(asList(properties.getProperty("storesCSV").split(",")));

        config.setExecutorService(createExecutor(properties));

        config.setClientMap(createClientMap(properties));

        config.setStatisticsSummary(new StatisticsSummary(config.getStores()));

        InputStream requestsInputStream = new FileInputStream(properties.getProperty("filePath"));

        config.setLines(new BufferedReader(new InputStreamReader(requestsInputStream)).lines());
    }

    @Data
    static class Config {
        private ExecutorService executorService;
        private RetryPolicy<Object> retryPolicy;
        private List<String> stores;
        private Map<String, VoldemortClient<?>> clientMap;
        private Stream<String> lines;
        private int sleepDurationBeforeRequest;
        private StatisticsSummary statisticsSummary;
        private int logInterval;
        private int reportSlowFetchTimeout;
        private int reportSizeIfGreaterThanBytes;
    }
}
