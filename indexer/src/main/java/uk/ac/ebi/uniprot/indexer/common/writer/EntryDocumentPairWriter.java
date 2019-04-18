package uk.ac.ebi.uniprot.indexer.common.writer;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.model.EntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.search.SolrCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@Slf4j
public abstract class EntryDocumentPairWriter<E, D> implements ItemWriter<EntryDocumentPair<E, D>> {
    private static final Logger INDEXING_FAILED_LOGGER = getLogger("indexing-doc-write-failed-entries");
    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private final RetryPolicy<Object> retryPolicy;
    private AtomicInteger failedWritingEntriesCount;
    private AtomicInteger writtenEntriesCount;

    public EntryDocumentPairWriter(SolrTemplate solrTemplate, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        this.solrTemplate = solrTemplate;
        this.collection = collection;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public void write(List<? extends EntryDocumentPair<E, D>> entryDocumentPairs) {
        List<D> documents = entryDocumentPairs.stream()
                .map(EntryDocumentPair::getDocument)
                .collect(Collectors.toList());

        try {
            Failsafe.with(retryPolicy)
                    .onFailure(failure -> logFailedEntriesToFile(entryDocumentPairs, failure.getFailure()))
                    .run(() -> writeEntriesToSolr(documents));
        } catch (Exception e) {
            List<String> accessions = documents.stream().map(this::extractDocumentId).collect(Collectors.toList());
            log.error("Error converting entry chunk: " + accessions, e);
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();

        this.failedWritingEntriesCount = new AtomicInteger(0);
        this.writtenEntriesCount = new AtomicInteger(0);

        executionContext
                .put(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_COUNT_KEY, this.failedWritingEntriesCount);
        executionContext
                .put(Constants.UNIPROTKB_INDEX_WRITTEN_ENTRIES_COUNT_KEY, this.writtenEntriesCount);
    }

    public abstract String extractDocumentId(D document);
    public abstract String entryToString(E entry);

    private void writeEntriesToSolr(List<D> documents) {
        solrTemplate.saveBeans(collection.name(), documents);
        writtenEntriesCount.addAndGet(documents.size());
    }

    private void logFailedEntriesToFile(List<? extends EntryDocumentPair<E, D>> entryDocumentPairs,
                                        Throwable throwable) {
        List<String> accessions = new ArrayList<>();
        for (EntryDocumentPair<E, D> entryDocumentPair : entryDocumentPairs) {
            String entryFF = entryToString(entryDocumentPair.getEntry());
            accessions.add(extractDocumentId(entryDocumentPair.getDocument()));
            INDEXING_FAILED_LOGGER.error(entryFF);
        }

        log.error("Error converting entry chunk: " + accessions, throwable);
        failedWritingEntriesCount.addAndGet(entryDocumentPairs.size());
    }
}
