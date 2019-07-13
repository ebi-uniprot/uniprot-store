package uk.ac.ebi.uniprot.indexer.common.concurrency;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created 13/07/19
 *
 * @author Edd
 */
@Slf4j
public class OnZeroCountSleeper {
    private static final int SLEEP_MILLIS = 5 * 1000;
    private static final int TIMEOUT_MILLIS_MAX = 60 * 1000;
    private final int timeoutMillisMax;
    private final AtomicInteger counter;

    public OnZeroCountSleeper() {
        this.counter = new AtomicInteger(0);
        this.timeoutMillisMax =  TIMEOUT_MILLIS_MAX;
    }

    public OnZeroCountSleeper(int initialCount, int timeoutMillisMax) {
        this.counter = new AtomicInteger(initialCount);
        this.timeoutMillisMax = timeoutMillisMax;
    }

    public int add(int delta) {
        return counter.addAndGet(delta);
    }

    public int increment() {
        return counter.incrementAndGet();
    }

    public int minus(int delta) {
        return counter.addAndGet(-delta);
    }

    public int decrement() {
        return counter.addAndGet(-1);
    }

    public int getCount() {
        return counter.get();
    }

    public void sleepUntilZero() {
        int timeoutMillisCounter = 0;

        while (counter.get() > 0) {
            log.info("Waiting {} ms for counter to reach 0", SLEEP_MILLIS);
            if (timeoutMillisCounter > timeoutMillisMax) {
                log.info("Sleeping stopped after timeout reached {} ms", timeoutMillisMax);
                break;
            }
            try {
                Thread.sleep(SLEEP_MILLIS);
            } catch (InterruptedException e) {
                // do nothing
            }
            timeoutMillisCounter += SLEEP_MILLIS;
        }
    }
}
