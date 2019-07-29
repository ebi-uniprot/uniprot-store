package uk.ac.ebi.uniprot.job.common.writer;

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
import uk.ac.ebi.uniprot.common.Utils;
import uk.ac.ebi.uniprot.common.concurrency.OnZeroCountSleeper;
import uk.ac.ebi.uniprot.job.common.store.Store;
import uk.ac.ebi.uniprot.job.common.util.CommonConstants;

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
public abstract class ItemRetryWriter<E, S> implements ItemWriter<E> {
    public static final String ITEM_WRITER_TASK_EXECUTOR = "itemWriterTaskExecutor";
    private static final Logger INDEXING_FAILED_LOGGER = getLogger("indexing-doc-write-failed-entries");
    private static final String ERROR_WRITING_ENTRIES_TO_STORE = "Error writing entries to Store: ";
    private final Store<S> store;
    private final RetryPolicy<Object> retryPolicy;
    private AtomicInteger failedWritingEntriesCount;
    private AtomicInteger writtenEntriesCount;
    private OnZeroCountSleeper sleeper;
    private ExecutionContext executionContext;

    public ItemRetryWriter(Store<S> store, RetryPolicy<Object> retryPolicy) {
        this.store = store;
        this.retryPolicy = retryPolicy;
    }

    @Override
    @Async(ITEM_WRITER_TASK_EXECUTOR)
    public void write(List<? extends E> items) {
        try {
            Failsafe.with(retryPolicy)
                    .onFailure(failure -> logFailedEntriesToFile(items, failure.getFailure()))
                    .run(() -> writeEntriesToStore(items));
        } catch (Exception e) {
            // already logged error
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        this.executionContext = stepExecution.getJobExecution().getExecutionContext();

        this.failedWritingEntriesCount = new AtomicInteger(0);
        this.writtenEntriesCount = new AtomicInteger(0);

        executionContext
                .put(CommonConstants.FAILED_ENTRIES_COUNT_KEY, this.failedWritingEntriesCount);
        executionContext
                .put(CommonConstants.WRITTEN_ENTRIES_COUNT_KEY, this.writtenEntriesCount);
    }

    private void recordItemsWereProcessed(int numberOfItemsProcessed) {
        if (!Utils.nonNull(sleeper)) {
            this.sleeper = (OnZeroCountSleeper) executionContext.get(CommonConstants.ENTRIES_TO_WRITE_COUNTER);
        }
        sleeper.minus(numberOfItemsProcessed);
    }

    public abstract String extractItemId(E item);

    public abstract String entryToString(E entry);

    public abstract S itemToEntry(E item);

    private void writeEntriesToStore(List<? extends E> items) {
        List<S> convertedItems = items.stream()
                .map(this::itemToEntry)
                .collect(Collectors.toList());
        store.save(convertedItems);
        writtenEntriesCount.addAndGet(convertedItems.size());
        recordItemsWereProcessed(convertedItems.size());
    }

    private void logFailedEntriesToFile(List<? extends E> items,
                                        Throwable throwable) {
        List<String> accessions = new ArrayList<>();
        for (E item : items) {
            String entryFF = entryToString(item);
            accessions.add(extractItemId(item));
            INDEXING_FAILED_LOGGER.error(entryFF);
        }

        log.error(ERROR_WRITING_ENTRIES_TO_STORE + accessions, throwable);
        failedWritingEntriesCount.addAndGet(items.size());
        recordItemsWereProcessed(items.size());
    }
}