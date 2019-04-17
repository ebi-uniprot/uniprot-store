package uk.ac.ebi.uniprot.indexer.uniprotkb.writer;

import lombok.extern.slf4j.Slf4j;
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
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;

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
public class UniProtEntryDocumentPairWriter implements ItemWriter<UniProtEntryDocumentPair> {
    private static final Logger INDEXING_FAILED_LOGGER = getLogger("indexing-doc-write-failed-entries");
    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private final RetryPolicy<Object> retryPolicy;
    private AtomicInteger failedWritingEntriesCount;
    private AtomicInteger writtenEntriesCount;

    public UniProtEntryDocumentPairWriter(SolrTemplate solrTemplate, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        this.solrTemplate = solrTemplate;
        this.collection = collection;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public void write(List<? extends UniProtEntryDocumentPair> convertibleEntries) {
        List<UniProtDocument> uniProtDocuments = convertibleEntries.stream()
                .map(UniProtEntryDocumentPair::getDocument)
                .collect(Collectors.toList());

        try {
            Failsafe.with(retryPolicy)
                    .onFailure(failure -> logFailedEntriesToFile(convertibleEntries, failure.getFailure()))
                    .run(() -> writeEntriesToSolr(uniProtDocuments));
        } catch (Exception e) {
            List<String> accessions = uniProtDocuments.stream().map(doc -> doc.accession).collect(Collectors.toList());
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

    private void writeEntriesToSolr(List<UniProtDocument> uniProtDocuments) {
        solrTemplate.saveBeans(collection.name(), uniProtDocuments);
        writtenEntriesCount.addAndGet(uniProtDocuments.size());
    }

    private void logFailedEntriesToFile(List<? extends UniProtEntryDocumentPair> uniProtEntryDocumentPairs,
                                        Throwable throwable) {
        List<String> accessions = new ArrayList<>();
        for (UniProtEntryDocumentPair uniProtEntryDocumentPair : uniProtEntryDocumentPairs) {
            String entryFF = UniProtFlatfileWriter.write(uniProtEntryDocumentPair.getEntry());
            accessions.add(uniProtEntryDocumentPair.getDocument().accession);
            INDEXING_FAILED_LOGGER.error(entryFF);
        }

        log.error("Error converting entry chunk: " + accessions, throwable);
        failedWritingEntriesCount.addAndGet(uniProtEntryDocumentPairs.size());
    }
}
