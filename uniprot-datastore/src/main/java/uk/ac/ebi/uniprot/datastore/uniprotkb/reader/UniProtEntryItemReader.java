package uk.ac.ebi.uniprot.datastore.uniprotkb.reader;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.common.concurrency.OnZeroCountSleeper;
import uk.ac.ebi.uniprot.datastore.uniprotkb.config.UniProtKBStoreProperties;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.flatfile.parser.impl.DefaultUniProtEntryIterator;
import uk.ac.ebi.uniprot.job.common.util.CommonConstants;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class UniProtEntryItemReader implements ItemReader<UniProtEntry> {
    private final DefaultUniProtEntryIterator entryIterator;
    private final OnZeroCountSleeper sleeper;

    public UniProtEntryItemReader(UniProtKBStoreProperties indexingProperties) {
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
    public UniProtEntry read() {
        if (entryIterator.hasNext()) {
            sleeper.increment();
            return entryIterator.next();
        } else {
            return null;
        }
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(CommonConstants.ENTRIES_TO_WRITE_COUNTER, sleeper);
    }
}
