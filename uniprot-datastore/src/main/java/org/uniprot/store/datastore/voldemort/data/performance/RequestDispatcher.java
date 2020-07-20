package org.uniprot.store.datastore.voldemort.data.performance;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Created 19/06/2020
 *
 * @author Edd
 */
@Slf4j
class RequestDispatcher {
    private final PerformanceChecker.Config config;
    private final RequestExecutor requestExecutor;

    RequestDispatcher(PerformanceChecker.Config config) {
        this.config = config;
        requestExecutor = new RequestExecutor(config);
    }

    void run() {
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
                                                    doRequest(storeRequestInfo);
                                                }));
    }

    void doRequest(StoreRequestInfo storeRequestInfo) {
        requestExecutor.performRequest(storeRequestInfo);
    }

    void printStatisticsSummary() {
        config.getStatisticsSummary().print();
    }

    RequestDispatcher.StoreRequestInfo parseLine(String line) {
        String[] parts = line.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Store line info should be a of the form: STORE_NAME,ID");
        }

        return RequestDispatcher.StoreRequestInfo.builder().store(parts[0]).id(parts[1]).build();
    }

    @Builder
    @Getter
    static class StoreRequestInfo {
        private final String store;
        private final String id;
    }
}
