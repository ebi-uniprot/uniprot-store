package uk.ac.ebi.uniprot.indexer.common.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class used to log statistics of the rate of writing. The primary purpose is to provide
 * a basis for reviewing, comparing and tuning batch run performances.
 * <p>
 * Created 17/04/19
 *
 * @author Edd
 */
@Slf4j
public class WriteRetrierLogRateListener<O> implements ItemWriteListener<O> {
    static final int WRITE_RATE_DOCUMENT_INTERVAL = 100000;
    private int writeRateDocumentInterval;
    private final Instant startOfWriting;
    private AtomicInteger totalWriteCount = new AtomicInteger(0);
    private AtomicInteger deltaWriteCount = new AtomicInteger(0);
    private Instant startOfDelta;

    public WriteRetrierLogRateListener() {
        this(Instant.now());
    }

    WriteRetrierLogRateListener(Instant now) {
        startOfWriting = startOfDelta = now;
        writeRateDocumentInterval = WRITE_RATE_DOCUMENT_INTERVAL;
    }

    @Override
    public void beforeWrite(List<? extends O> list) {
        // no-op
    }

    @Override
    public void afterWrite(List<? extends O> list) {
        deltaWriteCount.addAndGet(list.size());

        if (deltaWriteCount.get() >= writeRateDocumentInterval) {
            log.info(computeWriteRateStats(Instant.now()).toString());
            resetDelta();
        }
    }

    @Override
    public void onWriteError(Exception e, List<? extends O> list) {

    }

    /**
     * Compute writing rate statistics and return a formatted {@link StatsInfo} instance,
     * ready for printing.
     *
     * @param now the time point at which the statistics should be computed
     * @return a {@link StatsInfo} instance representing the write rate statistics
     */
    StatsInfo computeWriteRateStats(Instant now) {
        totalWriteCount.addAndGet(deltaWriteCount.get());

        StatsInfo statsInfo = new StatsInfo();
        statsInfo.totalWriteCount = totalWriteCount.get();
        statsInfo.totalSeconds = Duration.between(startOfWriting, now).getSeconds();
        statsInfo.deltaWriteCount = deltaWriteCount.get();
        statsInfo.deltaSeconds = Duration.between(startOfDelta, now).getSeconds();

        return statsInfo;
    }

    private void resetDelta() {
        deltaWriteCount.set(0);
        startOfDelta = Instant.now();
    }

    static class StatsInfo {
        private static final int SECONDS_IN_AN_HOUR = 3600;

        int deltaWriteCount;
        long deltaSeconds;

        int totalWriteCount;
        long totalSeconds;

        @Override
        public String toString() {
            float deltaDocsPerSecond = (float) deltaWriteCount / deltaSeconds;
            float totalDocsPerSecond = (float) totalWriteCount / totalSeconds;
            return
                    "\n\tWrite statistics {\n" +
                            "\t\tLatest delta:\n" +
                            String.format("\t\t\t# docs\t\t:\t%d%n", deltaWriteCount) +
                            String.format("\t\t\ttime (sec)\t:\t%d%n", deltaSeconds) +
                            String.format("\t\t\tdocs/sec\t:\t%.2f%n", deltaDocsPerSecond) +
                            String.format("\t\t\tdocs/hour\t:\t%.0f\t(projected from docs/sec)%n", deltaDocsPerSecond
                                    * SECONDS_IN_AN_HOUR) +
                            "\t\tOverall:\n" +
                            String.format("\t\t\t# docs\t\t:\t%d%n", totalWriteCount) +
                            String.format("\t\t\ttime (sec)\t:\t%d%n", totalSeconds) +
                            String.format("\t\t\tdocs/sec\t:\t%.2f%n", totalDocsPerSecond) +
                            String.format("\t\t\tdocs/hour\t:\t%.0f\t(projected from docs/sec)%n", totalDocsPerSecond *
                                    SECONDS_IN_AN_HOUR) +
                            "\t}\n";
        }
    }

}
