package uk.ac.ebi.uniprot.indexer.uniprotkb.reader;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.flatfile.parser.impl.DefaultUniProtEntryIterator;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class UniProtEntryItemReader implements ItemReader<UniProtEntryDocumentPair> {
    private final DefaultUniProtEntryIterator entryIterator;
    private final AtomicInteger entriesToWriteCounter;

    public UniProtEntryItemReader(UniProtKBIndexingProperties indexingProperties) {
        DefaultUniProtEntryIterator uniProtEntryIterator =
                new DefaultUniProtEntryIterator(indexingProperties.getEntryIteratorThreads(),
                                                indexingProperties.getEntryIteratorQueueSize(),
                                                indexingProperties.getEntryIteratorFFQueueSize());
        uniProtEntryIterator.setInput(indexingProperties.getUniProtEntryFile(),
                                      indexingProperties.getKeywordFile(),
                                      indexingProperties.getDiseaseFile(),
                                      indexingProperties.getAccessionGoPubmedFile(),
                                      indexingProperties.getSubcellularLocationFile());
        this.entryIterator = uniProtEntryIterator;
        this.entriesToWriteCounter = new AtomicInteger(0);
    }

    @Override
    public UniProtEntryDocumentPair read() {
        if (entryIterator.hasNext()) {
            entriesToWriteCounter.getAndIncrement();
            return new UniProtEntryDocumentPair(entryIterator.next());
        } else {
            return null;
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.ENTRIES_TO_WRITE_COUNTER, entriesToWriteCounter);
    }
}
