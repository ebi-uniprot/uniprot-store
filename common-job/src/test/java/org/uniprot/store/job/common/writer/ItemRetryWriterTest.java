package org.uniprot.store.job.common.writer;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.store.job.common.model.AbstractEntryDocumentPair;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.util.CommonConstants;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

/**
 * Created 28/07/19
 *
 * @author Edd
 */
@Slf4j
class ItemRetryWriterTest {
    private RetryPolicy<Object> retryPolicy;
    private FakeStore fakeStoreMock;
    private ItemRetryWriter<FakeEntry, FakeEntry> writer;

    @BeforeEach
    void beforeEach() {
        this.retryPolicy = new RetryPolicy<>().withMaxRetries(2);
        this.fakeStoreMock = mock(FakeStore.class);
        this.writer =
                new WriterUnderTest(items -> fakeStoreMock.saveToStore(items), this.retryPolicy);
        JobExecution mockJobExecution = new JobExecution(1L);
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.put(CommonConstants.ENTRIES_TO_WRITE_COUNTER, new OnZeroCountSleeper());
        mockJobExecution.setExecutionContext(executionContext);
        this.writer.setStepExecution(new StepExecution("fake step", mockJobExecution));
    }

    @Test
    void whenWriteItemsStoreIsCalled() {
        List<FakeEntry> chunk = createFakeEntries(4);
        this.writer.write(chunk);
        verify(fakeStoreMock, times(1)).saveToStore(chunk);
    }

    private List<FakeEntryDocPair> createFakePairs(int size) {
        List<FakeEntryDocPair> pairs = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            pairs.add(new FakeEntryDocPair(new FakeEntry("entry " + i)));
        }
        return pairs;
    }

    private List<FakeEntry> createFakeEntries(int size) {
        List<FakeEntry> pairs = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            pairs.add(new FakeEntry("id:" + i));
        }
        return pairs;
    }

    private static class WriterUnderTest extends ItemRetryWriter<FakeEntry, FakeEntry> {
        private WriterUnderTest(Store store, RetryPolicy<Object> retryPolicy) {
            super(store, retryPolicy);
        }

        @Override
        public String extractItemId(FakeEntry item) {
            return item.entryId;
        }

        @Override
        public String entryToString(FakeEntry entry) {
            return "{entryId: \"" + entry.entryId + "\"}";
        }

        @Override
        public FakeEntry itemToEntry(FakeEntry item) {
            return item;
        }
    }

    private static class FakeEntry {
        FakeEntry(String entryId) {
            this.entryId = entryId;
        }

        String entryId;
    }

    private class FakeDoc {
        FakeDoc(String docId) {
            this.docId = docId;
        }

        String docId;
    }

    private class FakeEntryDocPair extends AbstractEntryDocumentPair<FakeEntry, FakeDoc> {
        FakeEntryDocPair(FakeEntry entry) {
            super(entry);
        }
    }

    private static class FakeStore {
        void saveToStore(Collection<?> items) {
            log.info("Pretending to save items: {}", items);
        }
    }
}
