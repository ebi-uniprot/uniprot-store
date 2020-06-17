package org.uniprot.store.datastore.voldemort.data;

import lombok.Builder;
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
 * Created 12/06/2020
 *
 * @author Edd
 */
@Slf4j
public class PerformanceChecker {

    private static final List<String> PROPERTY_KEYS =
            asList(
                    "storeFetchRetryDelayMillis",
                    "storeFetchMaxRetries",
                    "corePoolSize",
                    "maxPoolSize",
                    "keepAliveTime",
                    "storesCSV",
                    "filePath");
    private static String propertiesFile;

    private static RetryPolicy<Object> retryPolicy;
    private static List<String> stores;
    private static ExecutorService executorService;
    private static Map<String, VoldemortClient<?>> clientMap;
    private static Stream<String> lines;

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            log.error("Please supply " + propertiesFile);
            System.exit(1);
        } else {
            propertiesFile = args[0];
        }

        final Properties properties = new Properties();

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

            retryPolicy =
                    new RetryPolicy<>()
                            .handle(IOException.class)
                            .withDelay(
                                    Duration.ofMillis(
                                            Long.parseLong(
                                                    properties.getProperty(
                                                            "storeFetchRetryDelayMillis"))))
                            .withMaxRetries(
                                    Integer.parseInt(
                                            properties.getProperty("storeFetchMaxRetries")));

            executorService = createExecutor(properties);

            stores = asList(properties.getProperty("storesCSV").split(","));

            clientMap = createClientMap(properties);

            InputStream requestsInputStream =
                    new FileInputStream(properties.getProperty("filePath"));

            lines = new BufferedReader(new InputStreamReader(requestsInputStream)).lines();
        } catch (IOException e) {
            log.error("Problem loading " + propertiesFile, e);
            System.exit(1);
        }

        // do requests
        RequestDispatcher dispatcher = new RequestDispatcher();
        dispatcher.go();
        executorService.shutdown();
        executorService.awaitTermination(48, TimeUnit.DAYS);

        // show statistics
        dispatcher.printStatisticsSummary();
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
        private final StatisticsSummary statisticsSummary;

        RequestDispatcher() {
            this.statisticsSummary = new StatisticsSummary(stores);
        }

        void go() {

            // get
            lines.map(this::parseLine)
                    .forEach(
                            storeRequestInfo ->
                                    executorService.execute(
                                            () -> {
                                                VoldemortClient<?> client =
                                                        clientMap.get(storeRequestInfo.getStore());
                                                RequestExecutor.performRequest(
                                                        client,
                                                        storeRequestInfo.getId(),
                                                        statisticsSummary);
                                            }));
        }

        void printStatisticsSummary() {
            statisticsSummary.print();
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
        static void performRequest(
                VoldemortClient<?> client, String id, StatisticsSummary summary) {
            LocalDateTime start = LocalDateTime.now();
            Failsafe.with(retryPolicy)
                    .onFailure(
                            throwable -> {
                                summary.getTotalFailed(client.getStoreName()).getAndIncrement();
                                log.error(
                                        "Failed to fetch id [store="
                                                + client.getStoreName()
                                                + ", id="
                                                + id
                                                + "]");
                            })
                    .onSuccess(
                            listener -> {
                                summary.getTotalRetrieved(client.getStoreName()).getAndIncrement();
                                long duration =
                                        Duration.between(start, LocalDateTime.now()).toMillis();
                                summary.getTotalFetchDuration(client.getStoreName())
                                        .getAndAdd(duration);
                                if (duration > 5000) {
                                    log.info(
                                            "Slow fetch [store="
                                                    + client.getStoreName()
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
                            totalFetchDurationForStore.get(storeName).get()
                                    + "\t\tAve successful request duration (ms) = "
                                    + totalFetchDurationForStore.get(storeName).get()
                                            / totalRetrievedForStore.get(storeName).get());
                }
            }
            log.info("=================== Statistics Summary End ===================");
        }
    }
}
