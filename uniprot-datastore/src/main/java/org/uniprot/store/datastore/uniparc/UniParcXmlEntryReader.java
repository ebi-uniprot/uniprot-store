package org.uniprot.store.datastore.uniparc;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.job.common.reader.XmlItemReader;
import org.uniprot.store.job.common.util.CommonConstants;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
public class UniParcXmlEntryReader extends XmlItemReader<Entry> {

    private static final String UNIPARC_ROOT_ELEMENT = "entry";
    private final OnZeroCountSleeper sleeper;

    public UniParcXmlEntryReader(String filepath) {
        super(filepath, Entry.class, UNIPARC_ROOT_ELEMENT);
        this.sleeper = new OnZeroCountSleeper();
    }

    @Override
    public Entry read() {
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
