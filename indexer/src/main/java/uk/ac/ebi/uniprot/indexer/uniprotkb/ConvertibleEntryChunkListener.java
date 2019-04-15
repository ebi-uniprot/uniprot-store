package uk.ac.ebi.uniprot.indexer.uniprotkb;

import lombok.EqualsAndHashCode;
import org.slf4j.Logger;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * If there is a failure to write the document, then print the entry to a file for future reference.
 * <p>
 * Created 12/04/19
 *
 * @author Edd
 */
public class ConvertibleEntryChunkListener implements ChunkListener {
    private static final Logger INDEXING_FAILED_LOGGER = getLogger("indexing-doc-write-failed-entries");
    private static final Logger LOGGER = getLogger(ConvertibleEntryChunkListener.class);
    private final int retryLimit;
    private StepExecution stepExecution;

    public ConvertibleEntryChunkListener(UniProtKBIndexingProperties indexingProperties) {
        this.retryLimit = indexingProperties.getRetryLimit();
    }

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        // no-op
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        // no-op
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        List<? extends ConvertibleEntry> entriesToLog = getEntriesToLog(executionContext);

        if (Objects.nonNull(entriesToLog)) {
            FailureCounter failureCounter = getFailureCounter(executionContext, entriesToLog);

            if (failureCounter.getFailedCount() == retryLimit || retryLimit <= 0) {
                logFailedEntriesToFile(entriesToLog);
            }

            failureCounter.increment();
        } else {
            // TODO: 15/04/19 use logger
            System.out.println("ExecutionContext does not contain chunk of previously written entries");
            executionContext.remove(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_CHUNK_KEY);
            executionContext.remove(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_WRITTEN_TO_FILE_KEY);
        }
    }

    @SuppressWarnings("unchecked")
    private List<? extends ConvertibleEntry> getEntriesToLog(ExecutionContext executionContext) {
        return (List<? extends ConvertibleEntry>)
                    executionContext.get(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_CHUNK_KEY);
    }

    private FailureCounter getFailureCounter(ExecutionContext executionContext, List<? extends ConvertibleEntry> convertibleEntries) {
        FailureCounter failureCounter = (FailureCounter) executionContext
                .get(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_WRITTEN_TO_FILE_KEY);
        if (Objects.isNull(failureCounter)) {
            failureCounter = new FailureCounter(convertibleEntries.hashCode());
            executionContext.put(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_WRITTEN_TO_FILE_KEY, failureCounter);
        }
        return failureCounter;
    }

    private void logFailedEntriesToFile(List<? extends ConvertibleEntry> convertibleEntries) {
        for (ConvertibleEntry convertibleEntry : convertibleEntries) {
            String entryFF = UniProtFlatfileWriter.write(convertibleEntry.getEntry());
            INDEXING_FAILED_LOGGER.error(entryFF);
        }
    }

    @BeforeStep
    public void getStepExecution(final StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    @EqualsAndHashCode
    private static class FailureCounter {
        private final int id;
        private final AtomicInteger failedCount;

        private FailureCounter(int id) {
            this.id = id;
            this.failedCount = new AtomicInteger(1);
        }

        private void increment() {
            this.failedCount.incrementAndGet();
        }

        private int getFailedCount() {
            return this.failedCount.get();
        }

        private int getId() {
            return this.id;
        }
    }
}
