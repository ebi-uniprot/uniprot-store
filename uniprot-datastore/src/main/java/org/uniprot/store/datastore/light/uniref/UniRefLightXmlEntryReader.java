package org.uniprot.store.datastore.light.uniref;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.job.common.reader.XmlItemReader;
import org.uniprot.store.job.common.util.CommonConstants;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
public class UniRefLightXmlEntryReader extends XmlItemReader<Entry> {

    public static final String UNIREF_ROOT_ELEMENT = "entry";
    private final OnZeroCountSleeper sleeper;

    public UniRefLightXmlEntryReader(String filepath) {
        super(filepath, Entry.class, UNIREF_ROOT_ELEMENT);
        this.sleeper = new OnZeroCountSleeper();
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(CommonConstants.ENTRIES_TO_WRITE_COUNTER, sleeper);
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
}
