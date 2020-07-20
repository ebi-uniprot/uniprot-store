package org.uniprot.store.datastore.voldemort.data.performance;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;

import org.github.jamm.MemoryMeter;
import org.uniprot.store.datastore.voldemort.VoldemortClient;

/**
 * Created 19/06/2020
 *
 * @author Edd
 */
@Slf4j
class RequestExecutor {
    private static final int BYTES_IN_ONE_MEGABYTE = 1000 * 1000;
    private final PerformanceChecker.Config config;

    RequestExecutor(PerformanceChecker.Config config) {
        this.config = config;
    }

    void performRequest(RequestDispatcher.StoreRequestInfo requestInfo) {
        LocalDateTime start = LocalDateTime.now();
        StatisticsSummary summary = config.getStatisticsSummary();
        VoldemortClient<?> client = config.getClientMap().get(requestInfo.getStore());
        String id = requestInfo.getId();
        String storeName = client.getStoreName();
        Failsafe.with(config.getRetryPolicy())
                .onFailure(
                        throwable -> {
                            summary.getTotalRequested().getAndIncrement();
                            summary.getTotalFailed(storeName).getAndIncrement();
                            logProgress();
                            log.error(
                                    "Failed to fetch id [store=" + storeName + ", id=" + id + "]");
                        })
                .onSuccess(
                        listener -> {
                            summary.getTotalRequested().getAndIncrement();
                            summary.getTotalRetrieved(storeName).getAndIncrement();
                            logProgress();
                            long duration = Duration.between(start, LocalDateTime.now()).toMillis();
                            summary.getTotalFetchDuration(storeName).getAndAdd(duration);
                            if (duration > config.getReportSlowFetchTimeout()) {
                                log.info(
                                        "Slow fetch [store="
                                                + storeName
                                                + ", id="
                                                + id
                                                + "]: "
                                                + duration
                                                + " ms");
                            }

                            printSizeIfBig(id, listener.getResult());
                        })
                .get(
                        () ->
                                client.getEntry(id)
                                        .orElseThrow(
                                                () ->
                                                        new IOException(
                                                                "Could not retrieve entry: "
                                                                        + id)));
    }

    void logProgress() {
        StatisticsSummary summary = config.getStatisticsSummary();
        int totalRequested = summary.getTotalRequested().get();
        if (totalRequested % config.getLogInterval() == 0) {
            log.info("Processed: " + totalRequested);
        }
    }

    String getSizeIfBigMessage(String id, Object o) {
        int greaterThanBytes = config.getReportSizeIfGreaterThanBytes();
        if (greaterThanBytes > 0) {
            long bytes = getSizeInBytes(o);
            double mbs = (double) bytes / BYTES_IN_ONE_MEGABYTE;

            if (bytes > greaterThanBytes) {
                return "Size of " + id + " = " + mbs + " mb";
            }
        }
        return null;
    }

    long getSizeInBytes(Object o) {
        return new MemoryMeter().measureDeep(o);
    }

    private void printSizeIfBig(String id, Object o) {
        String message = getSizeIfBigMessage(id, o);
        if (Objects.nonNull(message)) {
            log.info(message);
        }
    }
}
