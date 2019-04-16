package uk.ac.ebi.uniprot.indexer.uniprotkb;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.document.SolrCollection;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
public class ConvertibleEntryWriter implements ItemWriter<ConvertibleEntry> {
    private static final Logger INDEXING_FAILED_LOGGER = getLogger("indexing-doc-write-failed-entries");
    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private final RetryPolicy<Object> retryPolicy;
    private AtomicInteger failedWritingEntriesCount;
    private AtomicInteger writtenEntriesCount;

    public ConvertibleEntryWriter(SolrTemplate solrTemplate, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        this.solrTemplate = solrTemplate;
        this.collection = collection;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public void write(List<? extends ConvertibleEntry> convertibleEntries) {
        List<UniProtDocument> uniProtDocuments = convertibleEntries.stream()
                .map(ConvertibleEntry::getDocument)
                .collect(Collectors.toList());

        try {
            Failsafe.with(retryPolicy)
                    .onFailure(throwable -> logFailedEntriesToFile(convertibleEntries))
                    .run(() -> writeEntriesToSolr(uniProtDocuments));
        } catch(Throwable error){
            // do nothing because we have logged the entries that could not be written already
        }
    }

    private void writeEntriesToSolr(List<UniProtDocument> uniProtDocuments) {
        solrTemplate.saveBeans(collection.name(), uniProtDocuments);
        writtenEntriesCount.addAndGet(uniProtDocuments.size());
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

    private void logFailedEntriesToFile(List<? extends ConvertibleEntry> convertibleEntries) {
        for (ConvertibleEntry convertibleEntry : convertibleEntries) {
            String entryFF = UniProtFlatfileWriter.write(convertibleEntry.getEntry());
            INDEXING_FAILED_LOGGER.error(entryFF);
        }

        failedWritingEntriesCount.addAndGet(convertibleEntries.size());
    }
}
