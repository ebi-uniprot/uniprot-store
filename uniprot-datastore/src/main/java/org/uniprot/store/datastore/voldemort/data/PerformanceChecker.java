package org.uniprot.store.datastore.voldemort.data;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;

import java.io.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    private static final List<String> PROPERTY_KEYS =
            asList(
                    "sleepDurationBeforeRequest",
                    "logInterval",
                    "storeFetchRetryDelayMillis",
                    "storeFetchMaxRetries",
                    "corePoolSize",
                    "maxPoolSize",
                    "keepAliveTime",
                    "storesCSV",
                    "filePath");

    private static String propertiesFile;

    @Data
    private static class Config {
        private ExecutorService executorService;
        private RetryPolicy<Object> retryPolicy;
        private List<String> stores;
        private Map<String, VoldemortClient<?>> clientMap;
        private Stream<String> lines;
        private int sleepDurationBeforeRequest;
        private StatisticsSummary statisticsSummary;
        private int logInterval;
    }

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
        checker.init(properties, config);

        // do requests
        RequestDispatcher dispatcher = new RequestDispatcher(config);
        dispatcher.go();

        config.getExecutorService().shutdown();
        config.getExecutorService().awaitTermination(2, TimeUnit.DAYS);

        // show statistics
        dispatcher.printStatisticsSummary();
    }

    private void init(Properties properties, Config config) {
        try (InputStream stream = new FileInputStream(propertiesFile)) {
            properties.load(stream);

            PROPERTY_KEYS.forEach(
                    key -> {
                        if (!properties.containsKey(key)
                                && Objects.nonNull(properties.getProperty(key))) {
                            throw new IllegalArgumentException(
                                    "Must supply the '" + key + "' property");
                        }
                    });

            config.setSleepDurationBeforeRequest(
                    Integer.parseInt(properties.getProperty("sleepDurationBeforeRequest")));

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
                                    Integer.parseInt(
                                            properties.getProperty("storeFetchMaxRetries"))));

            config.setStores(asList(properties.getProperty("storesCSV").split(",")));

            config.setExecutorService(createExecutor(properties));

            config.setClientMap(createClientMap(properties));

            InputStream requestsInputStream =
                    new FileInputStream(properties.getProperty("filePath"));

            config.setLines(new BufferedReader(new InputStreamReader(requestsInputStream)).lines());
        } catch (IOException e) {
            log.error("Problem loading " + propertiesFile, e);
            System.exit(1);
        }
    }

    private static Map<String, VoldemortClient<?>> createClientMap(Properties properties) {
        Map<String, VoldemortClient<?>> map = new HashMap<>();

        String uniProtKBStoreName = properties.getProperty("store.uniprotkb.storeName");
        if (uniProtKBStoreName != null) {
            map.put(
                    uniProtKBStoreName,
                    new VoldemortRemoteUniProtKBEntryStore(
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
                    new VoldemortRemoteUniRefEntryStore(
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
                    new VoldemortRemoteUniParcEntryStore(
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

    private static ThreadPoolExecutor createExecutor(Properties properties) {
        return new ThreadPoolExecutor(
                Integer.parseInt(properties.getProperty("corePoolSize")),
                Integer.parseInt(properties.getProperty("maxPoolSize")),
                Integer.parseInt(properties.getProperty("keepAliveTime")),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
    }

    private static class RequestDispatcher {
        private final PerformanceChecker.Config config;

        RequestDispatcher(Config config) {
            config.setStatisticsSummary(new StatisticsSummary(config.getStores()));
            this.config = config;
        }

        void go() {

            // get
            config.getLines()
                    .map(this::parseLine)
                    .forEach(
                            storeRequestInfo ->
                                    config.getExecutorService()
                                            .execute(
                                                    () -> {
                                                        try {
                                                            Thread.sleep(
                                                                    config
                                                                            .getSleepDurationBeforeRequest());
                                                        } catch (InterruptedException e) {
                                                            Thread.currentThread().interrupt();
                                                            log.warn("Problem whilst sleeping");
                                                        }
                                                        RequestExecutor.performRequest(
                                                                config, storeRequestInfo);
                                                    }));
        }

        void printStatisticsSummary() {
            config.getStatisticsSummary().print();
        }

        private StoreRequestInfo parseLine(String line) {
            String[] parts = line.split(",");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Store line info should be a of the form: STORE_NAME,ID");
            }

            return StoreRequestInfo.builder().store(parts[0]).id(parts[1]).build();
        }

        @Builder
        @Getter
        private static class StoreRequestInfo {
            private final String store;
            private final String id;
        }
    }

    private static class RequestExecutor {
        static void logProgress(Config config) {
            StatisticsSummary summary = config.getStatisticsSummary();
            int totalProcessed =
                    summary.getTotalRetrievedForStore().values().stream()
                                    .mapToInt(AtomicInteger::get)
                                    .sum()
                            + summary.getTotalFailedForStore().values().stream()
                                    .mapToInt(AtomicInteger::get)
                                    .sum();
            if (totalProcessed % config.getLogInterval() == 0) {
                log.info("Processed: " + totalProcessed);
            }
        }

        static void performRequest(Config config, RequestDispatcher.StoreRequestInfo requestInfo) {
            LocalDateTime start = LocalDateTime.now();
            StatisticsSummary summary = config.getStatisticsSummary();
            VoldemortClient<?> client = config.getClientMap().get(requestInfo.getStore());
            String id = requestInfo.getId();
            String storeName = client.getStoreName();
            Failsafe.with(config.getRetryPolicy())
                    .onFailure(
                            throwable -> {
                                summary.getTotalFailed(storeName).getAndIncrement();
                                logProgress(config);
                                log.error(
                                        "Failed to fetch id [store="
                                                + storeName
                                                + ", id="
                                                + id
                                                + "]");
                            })
                    .onSuccess(
                            listener -> {
                                summary.getTotalRetrieved(storeName).getAndIncrement();
                                logProgress(config);
                                long duration =
                                        Duration.between(start, LocalDateTime.now()).toMillis();
                                summary.getTotalFetchDuration(storeName).getAndAdd(duration);
                                if (duration > 5000) {
                                    log.info(
                                            "Slow fetch [store="
                                                    + storeName
                                                    + ", id="
                                                    + id
                                                    + "]: "
                                                    + duration
                                                    + " ms");
                                }
                            })
                    .run(
                            () ->
                                    client.getEntry(id)
                                            .orElseThrow(
                                                    () ->
                                                            new IOException(
                                                                    "Could not retrieve entry: "
                                                                            + id)));
        }
    }

    @Getter
    private static class StatisticsSummary {
        private static final LocalDateTime START = LocalDateTime.now();

        public AtomicInteger getTotalRetrieved(String store) {
            return totalRetrievedForStore.get(store);
        }

        public AtomicInteger getTotalFailed(String store) {
            return totalFailedForStore.get(store);
        }

        public AtomicLong getTotalFetchDuration(String store) {
            return totalFetchDurationForStore.get(store);
        }

        private final Map<String, AtomicInteger> totalRetrievedForStore = new HashMap<>();
        private final Map<String, AtomicInteger> totalFailedForStore = new HashMap<>();
        private final Map<String, AtomicLong> totalFetchDurationForStore = new HashMap<>();

        StatisticsSummary(Collection<String> stores) {
            stores.forEach(
                    store -> {
                        totalRetrievedForStore.put(store, new AtomicInteger());
                        totalFailedForStore.put(store, new AtomicInteger());
                        totalFetchDurationForStore.put(store, new AtomicLong());
                    });
        }

        public void print() {
            DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM);
            LocalDateTime end = LocalDateTime.now();
            Duration duration = Duration.between(START, end);

            log.info("=================== Statistics Summary Start ===================");
            log.info("\tStart = " + START.format(formatter));
            log.info("\tEnd = " + end.format(formatter));
            log.info("\tDuration: " + duration.getSeconds() + " seconds");

            for (Map.Entry<String, AtomicInteger> entry : totalRetrievedForStore.entrySet()) {
                String storeName = entry.getKey();
                double failedPercentage =
                        (double) (100 * totalFailedForStore.get(storeName).get())
                                / (totalRetrievedForStore.get(storeName).get()
                                        + totalFailedForStore.get(storeName).get());
                log.info("\t" + storeName);
                log.info("\t\tTotal retrieved successfully = " + entry.getValue().get());
                log.info(
                        "\t\tTotal failed = "
                                + totalFailedForStore.get(storeName)
                                + " ("
                                + failedPercentage
                                + " %)");
                if (totalRetrievedForStore.get(storeName).get() != 0) {
                    log.info(
                            "\t\tAve successful request duration (ms) = "
                                    + totalFetchDurationForStore.get(storeName).get()
                                            / totalRetrievedForStore.get(storeName).get());
                }
            }
            log.info("=================== Statistics Summary End ===================");
        }
    }
}
