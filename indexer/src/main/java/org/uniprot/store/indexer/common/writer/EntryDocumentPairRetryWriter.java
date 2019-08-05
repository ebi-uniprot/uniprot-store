package org.uniprot.store.indexer.common.writer;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.scheduling.annotation.Async;
import org.uniprot.core.util.Utils;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.listener.WriteRetrierLogJobListener;
import org.uniprot.store.indexer.common.listener.WriteRetrierLogStepListener;
import org.uniprot.store.indexer.common.model.EntryDocumentPair;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.SolrCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * <p>This abstract class is responsible for writing documents from a list of {@link EntryDocumentPair}s to Solr.
 * Writes to Solr are retried as configured in a {@link RetryPolicy} instance.</p>
 *
 * <p>Given the list of {@link EntryDocumentPair}s, if the constituent documents (obtained via {@link EntryDocumentPair#getDocument()})
 * cause an {@link Exception} when writing to Solr even after all attempted retries, then a dedicated log file is generated.
 * The log file is created as follows: for every entry in the list of {@link EntryDocumentPair}s (obtained via {@link EntryDocumentPair#getEntry()}
 * the method {@link #entryToString(Object)} is called. Concrete implementations of this class must override this method
 * to define how to write each entry in the log file.</p>
 *
 * <p>NOTE: this class catches failures via a simple retry framework ({@link RetryPolicy}) during writes. Therefore Spring
 * Batch records false statistics of numbers of skips, writes, etc. For this reason, when using this class in a {@link org.springframework.batch.core.Step},
 * there are the following recommendations:
 * <ul>
 * <li>When using this class, do not use Spring Batch's fault tolerance {@link Job} features.</li>
 * <li>Use {@link WriteRetrierLogStepListener}, {@link WriteRetrierLogJobListener} to capture write statistics.</li>
 * </ul>
 * </p>
 * Created 12/04/19
 *
 * @author Edd
 */
@Slf4j
public abstract class EntryDocumentPairRetryWriter<E, D, T extends EntryDocumentPair<E, D>> implements ItemWriter<T> {
    public static final String ITEM_WRITER_TASK_EXECUTOR = "itemWriterTaskExecutor";
    private static final Logger INDEXING_FAILED_LOGGER = getLogger("indexing-doc-write-failed-entries");
    private static final String ERROR_WRITING_ENTRIES_TO_SOLR = "Error writing entries to Solr: ";
    private final UniProtSolrOperations solrOperations;
    private final SolrCollection collection;
    private final RetryPolicy<Object> retryPolicy;
    private AtomicInteger failedWritingEntriesCount;
    private AtomicInteger writtenEntriesCount;
    private OnZeroCountSleeper sleeper;
    private ExecutionContext executionContext;

    public EntryDocumentPairRetryWriter(UniProtSolrOperations solrOperations, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        this.solrOperations = solrOperations;
        this.collection = collection;
        this.retryPolicy = retryPolicy;
    }

    @Override
    @Async(ITEM_WRITER_TASK_EXECUTOR)
    public void write(List<? extends T> entryDocumentPairs) {
        List<D> documents = entryDocumentPairs.stream()
                .map(EntryDocumentPair::getDocument)
                .collect(Collectors.toList());

        try {
            Failsafe.with(retryPolicy)
                    .onFailure(failure -> logFailedEntriesToFile(entryDocumentPairs, failure.getFailure()))
                    .run(() -> writeEntriesToSolr(documents));
        } catch (Exception e) {
            // already logged error
        }
    }

    private void recordItemsWereProcessed(int numberOfItemsProcessed) {
        if (!Utils.nonNull(sleeper)) {
            this.sleeper = (OnZeroCountSleeper) executionContext.get(Constants.ENTRIES_TO_WRITE_COUNTER);
        }
        sleeper.minus(numberOfItemsProcessed);
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        this.executionContext = stepExecution.getJobExecution().getExecutionContext();

        this.failedWritingEntriesCount = new AtomicInteger(0);
        this.writtenEntriesCount = new AtomicInteger(0);

        executionContext
                .put(Constants.INDEX_FAILED_ENTRIES_COUNT_KEY, this.failedWritingEntriesCount);
        executionContext
                .put(Constants.INDEX_WRITTEN_ENTRIES_COUNT_KEY, this.writtenEntriesCount);

    }

    public abstract String extractDocumentId(D document);

    public abstract String entryToString(E entry);

    private void writeEntriesToSolr(List<D> documents) {
        solrOperations.saveBeans(collection.name(), documents);
        writtenEntriesCount.addAndGet(documents.size());
        recordItemsWereProcessed(documents.size());
    }

    private void logFailedEntriesToFile(List<? extends EntryDocumentPair<E, D>> entryDocumentPairs,
                                        Throwable throwable) {
        List<String> accessions = new ArrayList<>();
        for (EntryDocumentPair<E, D> entryDocumentPair : entryDocumentPairs) {
            String entryFF = entryToString(entryDocumentPair.getEntry());
            accessions.add(extractDocumentId(entryDocumentPair.getDocument()));
            INDEXING_FAILED_LOGGER.error(entryFF);
        }

        log.error(ERROR_WRITING_ENTRIES_TO_SOLR + accessions, throwable);
        failedWritingEntriesCount.addAndGet(entryDocumentPairs.size());
        recordItemsWereProcessed(entryDocumentPairs.size());
    }
}
