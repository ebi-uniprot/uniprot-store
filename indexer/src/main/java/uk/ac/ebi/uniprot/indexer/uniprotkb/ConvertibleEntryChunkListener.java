package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

import java.util.List;
import java.util.Objects;

/**
 * If there is a failure to write the document, then print the entry to a file for future reference.
 * <p>
 * Created 12/04/19
 *
 * @author Edd
 */
// TODO: 13/04/19 make this work
public class ConvertibleEntryChunkListener implements ChunkListener {
    private static final Logger INDEXING_FAILED_LOGGER = LoggerFactory
            .getLogger("indexing-doc-write-failed-entries");
    private StepExecution stepExecution;

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        System.out.println("before chunk");
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        System.out.println("after chunk");
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        System.out.println("after chunk error");
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        List<? extends ConvertibleEntry> convertibleEntries = (List<? extends ConvertibleEntry>) executionContext
                .get(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_CHUNK_KEY);
        if (Objects.nonNull(convertibleEntries)) {
            for (ConvertibleEntry convertibleEntry : convertibleEntries) {
                String entryFF = UniProtFlatfileWriter.write(convertibleEntry.getEntry());
                INDEXING_FAILED_LOGGER.error(entryFF);
            }
        } else {
            System.out.println("ExecutionContext does not contain chunk of previously written entries");
        }
        executionContext.put(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_CHUNK_KEY, null);
    }

    @BeforeStep
    public void getStepExecution(final StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
