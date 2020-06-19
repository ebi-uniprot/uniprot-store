package org.uniprot.store.datastore.voldemort.data.performance;

import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * Created 19/06/2020
 *
 * @author Edd
 */
class RequestDispatcherTest {
    @Test
    void canRun() throws InterruptedException {
        // given
        PerformanceChecker.Config config = mock(PerformanceChecker.Config.class);
        TestableRequestDispatcher dispatcher = new TestableRequestDispatcher(config);

        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        1, 1, 1L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
        when(config.getExecutorService()).thenReturn(executor);
        when(config.getSleepDurationBeforeRequest()).thenReturn(10);
        when(config.getLines()).thenReturn(Stream.of("uniprotkb,P12345"));

        // when
        dispatcher.run();

        config.getExecutorService().shutdown();
        config.getExecutorService().awaitTermination(2, TimeUnit.SECONDS);

        // then
        assertThat(dispatcher.requestCalled, is(true));
    }

    @Test
    void printsSummary() {
        // given
        PerformanceChecker.Config config = mock(PerformanceChecker.Config.class);
        TestableRequestDispatcher dispatcher = new TestableRequestDispatcher(config);

        when(config.getStatisticsSummary()).thenReturn(mock(StatisticsSummary.class));

        // when
        dispatcher.printStatisticsSummary();

        // then
        verify(config.getStatisticsSummary()).print();
    }

    @Test
    void splitsLinesCorrectly() {
        TestableRequestDispatcher dispatcher = new TestableRequestDispatcher(null);

        RequestDispatcher.StoreRequestInfo storeRequestInfo =
                dispatcher.parseLine("uniprotkb,P12345");

        assertThat(storeRequestInfo.getStore(), is("uniprotkb"));
        assertThat(storeRequestInfo.getId(), is("P12345"));
    }

    @Test
    void incorrectLineCausesExceptionWhenSplit() {
        TestableRequestDispatcher dispatcher = new TestableRequestDispatcher(null);

        assertThrows(
                IllegalArgumentException.class,
                () -> dispatcher.parseLine("uniprotkbasdf;kljP12345"));
    }

    private static class TestableRequestDispatcher extends RequestDispatcher {
        private boolean requestCalled = false;

        TestableRequestDispatcher(PerformanceChecker.Config config) {
            super(config);
        }

        @Override
        void doRequest(StoreRequestInfo storeRequestInfo) {
            requestCalled = true;
        }
    }
}
