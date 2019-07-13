package uk.ac.ebi.uniprot.indexer.common.writer;

import net.jodah.failsafe.RetryPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.uniprot.indexer.common.concurrency.OnZeroCountSleeper;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.indexer.common.model.AbstractEntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.search.SolrCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;
import static uk.ac.ebi.uniprot.search.SolrCollection.uniprot;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
class EntryDocumentPairRetryWriterTest {
    private RetryPolicy<Object> retryPolicy;
    private UniProtSolrOperations solrOperationsMock;
    private EntryDocumentPairRetryWriter<FakeEntry, FakeDoc, FakeEntryDocPair> writer;

    @BeforeEach
    void beforeEach() {
        this.retryPolicy = new RetryPolicy<>().withMaxRetries(2);
        this.solrOperationsMock = mock(UniProtSolrOperations.class);
        this.writer = new WriterUnderTest(this.solrOperationsMock, uniprot, this.retryPolicy);
        JobExecution mockJobExecution = new JobExecution(1L);
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.put(Constants.ENTRIES_TO_WRITE_COUNTER, new OnZeroCountSleeper());
        mockJobExecution.setExecutionContext(executionContext);
        this.writer.setStepExecution(new StepExecution("fake step", mockJobExecution));
    }

    @Test
    void whenWriteItemsSolrIsCalled() {
        List<FakeEntryDocPair> chunk = createFakePairs(4);
        this.writer.write(chunk);
        verify(solrOperationsMock, times(1))
                .saveBeans(uniprot.name(), chunk.stream()
                        .map(AbstractEntryDocumentPair::getDocument).collect(Collectors.toList()));
    }

    private List<FakeEntryDocPair> createFakePairs(int size) {
        List<FakeEntryDocPair> pairs = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            pairs.add(new FakeEntryDocPair(new FakeEntry("entry " + i)));
        }
        return pairs;
    }

    private static class WriterUnderTest extends EntryDocumentPairRetryWriter<FakeEntry, FakeDoc, FakeEntryDocPair> {
        private WriterUnderTest(UniProtSolrOperations solrOperations, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
            super(solrOperations, collection, retryPolicy);
        }

        @Override
        public String extractDocumentId(FakeDoc document) {
            return document.docId;
        }

        @Override
        public String entryToString(FakeEntry entry) {
            return entry.entryId;
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
}
