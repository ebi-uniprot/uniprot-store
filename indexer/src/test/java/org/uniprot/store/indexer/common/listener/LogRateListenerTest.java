package org.uniprot.store.indexer.common.listener;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.store.indexer.common.listener.LogRateListener;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.uniprot.store.indexer.common.listener.LogRateListener.WRITE_RATE_DOCUMENT_INTERVAL;

/**
 * Created 17/04/19
 *
 * @author Edd
 */
class LogRateListenerTest {
    private Instant start;
    private LogRateListener<Object> itemRateWriterListener;

    private List<Object> mockedWrittenDocList;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockedWrittenDocList = (List<Object>)mock(List.class);
        start = Instant.now();
        itemRateWriterListener = new LogRateListener<>(start);
    }

    @Test
    void computesRateAfterOneWrite() throws Exception {
        int numDocs = 40;
        Instant fiveSecsAfterStart = start.plusSeconds(5);

        when(mockedWrittenDocList.size()).thenReturn(numDocs);

        itemRateWriterListener.afterWrite(mockedWrittenDocList);
        LogRateListener.StatsInfo statsInfo = itemRateWriterListener.computeWriteRateStats(fiveSecsAfterStart);

        System.out.println(statsInfo.toString());
        assertThat(statsInfo.totalSeconds, is(5L));
        assertThat(statsInfo.totalWriteCount, is(numDocs));
    }

    @Test
    void computesRateAfterMultipleWrites() throws Exception {
        int tenDocs = 10;
        long twoSeconds = 2L;
        Instant twoSecsAfterStart = start.plusSeconds(twoSeconds);

        when(mockedWrittenDocList.size()).thenReturn(tenDocs);
        itemRateWriterListener.afterWrite(mockedWrittenDocList);

        // add lots of docs to trigger a new delta
        when(mockedWrittenDocList.size()).thenReturn(WRITE_RATE_DOCUMENT_INTERVAL);
        itemRateWriterListener.afterWrite(mockedWrittenDocList);

        when(mockedWrittenDocList.size()).thenReturn(tenDocs);
        itemRateWriterListener.afterWrite(mockedWrittenDocList);

        LogRateListener.StatsInfo statsInfo = itemRateWriterListener.computeWriteRateStats(twoSecsAfterStart);

        System.out.println(statsInfo);
        assertThat(statsInfo.deltaWriteCount, is(tenDocs));
        // do not test delta time, because it internally uses
        assertThat(statsInfo.totalSeconds, is(twoSeconds));
        assertThat(statsInfo.totalWriteCount, is(tenDocs + tenDocs + WRITE_RATE_DOCUMENT_INTERVAL));
    }

}