package org.uniprot.store.datastore.voldemort.data.performance;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created 19/06/2020
 *
 * @author Edd
 */
@Getter
@Slf4j
class StatisticsSummary {
    private static final LocalDateTime START = LocalDateTime.now();
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM);
    private final AtomicInteger totalRequested = new AtomicInteger();
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

    public AtomicInteger getTotalRetrieved(String store) {
        return totalRetrievedForStore.get(store);
    }

    public AtomicInteger getTotalFailed(String store) {
        return totalFailedForStore.get(store);
    }

    public AtomicLong getTotalFetchDuration(String store) {
        return totalFetchDurationForStore.get(store);
    }

    public AtomicInteger getTotalRequested() {
        return totalRequested;
    }

    String getMessage() {
        LocalDateTime end = LocalDateTime.now();
        Duration duration = Duration.between(START, end);

        StringBuilder stringBuilder =
                new StringBuilder(
                                "\n=================== Statistics Summary Start ===================\n")
                        .append("\tStart = " + START.format(FORMATTER) + "\n")
                        .append("\tEnd = " + end.format(FORMATTER) + "\n")
                        .append("\tDuration: " + duration.getSeconds() + " seconds" + "\n");

        for (Map.Entry<String, AtomicInteger> entry : totalRetrievedForStore.entrySet()) {
            String storeName = entry.getKey();
            double failedPercentage =
                    (double) (100 * totalFailedForStore.get(storeName).get())
                            / (totalRetrievedForStore.get(storeName).get()
                                    + totalFailedForStore.get(storeName).get());
            stringBuilder
                    .append("\t" + storeName + "\n")
                    .append("\t\tTotal retrieved successfully = " + entry.getValue().get() + "\n")
                    .append(
                            "\t\tTotal failed = "
                                    + totalFailedForStore.get(storeName)
                                    + " ("
                                    + failedPercentage
                                    + " %)"
                                    + "\n");
            if (totalRetrievedForStore.get(storeName).get() != 0) {
                stringBuilder.append(
                        "\t\tAve successful request duration (ms) = "
                                + totalFetchDurationForStore.get(storeName).get()
                                        / totalRetrievedForStore.get(storeName).get()
                                + "\n");
            }
        }
        stringBuilder.append("=================== Statistics Summary End ===================\n");
        return stringBuilder.toString();
    }

    public void print() {
        log.info(getMessage());
    }
}
