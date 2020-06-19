package org.uniprot.store.datastore.voldemort.data.performance;

import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.uniprot.store.datastore.voldemort.VoldemortClient;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * Created 19/06/2020
 *
 * @author Edd
 */
@ExtendWith(MockitoExtension.class)
class RequestExecutorTest {
    @Mock private VoldemortClient<String> mockClient;

    @Test
    void canPerformRequest() {
        // given
        PerformanceChecker.Config config = new PerformanceChecker.Config();
        RequestExecutor executor = new RequestExecutor(config);

        StatisticsSummary mockSummary = mock(StatisticsSummary.class);
        config.setStatisticsSummary(mockSummary);

        config.setReportSizeIfGreaterThanBytes(-1);

        Map<String, VoldemortClient<?>> clientMap = new HashMap<>();
        String storeName = "uniprotkb";
        when(mockClient.getStoreName()).thenReturn(storeName);
        String id = "P12345";
        when(mockClient.getEntry(id)).thenReturn(Optional.of("hello world"));
        clientMap.put(storeName, mockClient);
        config.setClientMap(clientMap);

        config.setRetryPolicy(new RetryPolicy<>().handle(IOException.class));

        RequestDispatcher.StoreRequestInfo requestInfo =
                RequestDispatcher.StoreRequestInfo.builder().id(id).store(storeName).build();

        // when
        executor.performRequest(requestInfo);

        // then
        verify(mockClient).getEntry(id);
    }

    @Test
    void failedToFetchCausesException() {
        // given
        PerformanceChecker.Config config = new PerformanceChecker.Config();
        RequestExecutor executor = new RequestExecutor(config);

        String storeName = "uniprotkb";
        StatisticsSummary summary = new StatisticsSummary(Collections.singletonList(storeName));
        config.setStatisticsSummary(summary);

        config.setLogInterval(1);

        Map<String, VoldemortClient<?>> clientMap = new HashMap<>();
        when(mockClient.getStoreName()).thenReturn(storeName);
        String id = "P12345";
        when(mockClient.getEntry(id)).thenReturn(Optional.empty());
        clientMap.put(storeName, mockClient);
        config.setClientMap(clientMap);

        config.setRetryPolicy(
                new RetryPolicy<>()
                        .handle(IOException.class)
                        .withMaxRetries(1)
                        .withDelay(Duration.ofMillis(1)));

        RequestDispatcher.StoreRequestInfo requestInfo =
                RequestDispatcher.StoreRequestInfo.builder().id(id).store(storeName).build();

        // when
        assertThrows(
                FailsafeException.class,
                () -> {
                    executor.performRequest(requestInfo);
                    verify(mockClient).getEntry(id);
                    assertThat(summary.getTotalRequested().get(), is(1));
                    assertThat(summary.getTotalFailed(storeName).get(), is(1));
                    assertThat(summary.getTotalRetrieved(storeName).get(), is(0));
                });
    }

    @Test
    void canPrintSize() {
        // given
        PerformanceChecker.Config config = new PerformanceChecker.Config();

        config.setReportSizeIfGreaterThanBytes(10);
        String message =
                new TestableRequestExecutor(config, 1024L * 1000)
                        .getSizeIfBigMessage("anything", null);

        assertThat(message, is("Size of anything = 1.0 mb"));
    }

    private static class TestableRequestExecutor extends RequestExecutor {
        private final long size;

        TestableRequestExecutor(PerformanceChecker.Config config, long size) {
            super(config);
            this.size = size;
        }

        @Override
        long getSizeInBytes(Object o) {
            return size;
        }
    }
}
