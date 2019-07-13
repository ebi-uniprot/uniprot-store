package uk.ac.ebi.uniprot.indexer.uniprotkb.reader;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.flatfile.parser.impl.DefaultUniProtEntryIterator;
import uk.ac.ebi.uniprot.indexer.common.concurrency.OnZeroCountSleeper;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.uniprotkb.config.UniProtKBIndexingProperties;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class UniProtEntryItemReader implements ItemReader<UniProtEntryDocumentPair> {
    private final DefaultUniProtEntryIterator entryIterator;
    private final OnZeroCountSleeper sleeper;

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
        this.sleeper = new OnZeroCountSleeper();
    }

    @Override
    public UniProtEntryDocumentPair read() {
        if (entryIterator.hasNext()) {
            sleeper.increment();
            return new UniProtEntryDocumentPair(entryIterator.next());
        } else {
            return null;
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.ENTRIES_TO_WRITE_COUNTER, sleeper);
    }
}
