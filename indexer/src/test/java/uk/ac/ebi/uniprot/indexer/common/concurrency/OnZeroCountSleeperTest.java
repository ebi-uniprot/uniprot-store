package uk.ac.ebi.uniprot.indexer.common.concurrency;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Created 13/07/19
 *
 * @author Edd
 */
@Slf4j
class OnZeroCountSleeperTest {

    private OnZeroCountSleeper sleeper;

    @BeforeEach
    void setUp() {
        this.sleeper = new OnZeroCountSleeper();
    }

    @Test
    void canCreateDefaultSleeper() {
        assertThat(sleeper.getCount(), is(0));
    }

    @Test
    void canCreateSleeperWithInitialCount() {
        int initialCount = 6;
        OnZeroCountSleeper onZeroCountSleeper = new OnZeroCountSleeper(initialCount, 1000);
        assertThat(onZeroCountSleeper.getCount(), is(initialCount));
    }

    @Test
    void canIncrementSleeper() {
        sleeper.increment();
        assertThat(sleeper.getCount(), is(1));
    }

    @Test
    void canAddToSleeper() {
        int delta = 8;
        sleeper.add(delta);
        assertThat(sleeper.getCount(), is(delta));
    }

    @Test
    void canDecrementSleeper() {
        int delta = 8;
        sleeper.add(delta);
        sleeper.decrement();
        assertThat(sleeper.getCount(), is(delta - 1));
    }

    @Test
    void canMinusFromSleeper() {
        int plusDelta = 8;
        int minusDelta = 5;
        sleeper.add(plusDelta);
        sleeper.minus(minusDelta);
        assertThat(sleeper.getCount(), is(plusDelta - minusDelta));
    }

    @Test
    void willWaitUntilZero() {
        OnZeroCountSleeper sleeper = new OnZeroCountSleeper();
        List<Thread> incrementers = Stream
                .generate(() -> new Thread(() -> {
                    int count = sleeper.increment();
                    log.info("incremented, so that count = {}", count);
                }))
                .limit(5)
                .collect(toList());

        List<Thread> decrementers = Stream
                .generate(() -> new Thread(() -> {
                    try {
                        Thread.sleep((long) (Math.random() * 100));
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                    int count = sleeper.decrement();
                    log.info("decremented, so that count = {}", count);
                }))
                .limit(5)
                .collect(toList());

        incrementers.forEach(Thread::start);

        decrementers.forEach(Thread::start);

        sleeper.sleepUntilZero();

        assertThat(sleeper.getCount(), is(0));
    }

    @Test
    void willWaitOnlyUntilMaxTimeout() {
        OnZeroCountSleeper sleeper = new OnZeroCountSleeper(0, 1000);
        List<Thread> incrementers = Stream
                .generate(() -> new Thread(() -> {
                    int count = sleeper.increment();
                    log.info("incremented, so that count = {}", count);
                }))
                .limit(5)
                .collect(toList());

        List<Thread> decrementers = Stream
                .generate(() -> new Thread(() -> {
                    try {
                        Thread.sleep((long) (100000));
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                    int count = sleeper.decrement();
                    log.info("decremented, so that count = {}", count);
                }))
                .limit(5)
                .collect(toList());

        incrementers.forEach(Thread::start);

        decrementers.forEach(Thread::start);

        sleeper.sleepUntilZero();

        assertThat(sleeper, is(notNullValue()));
    }
}