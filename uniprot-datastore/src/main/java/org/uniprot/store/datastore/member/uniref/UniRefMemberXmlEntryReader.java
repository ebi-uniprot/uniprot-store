package org.uniprot.store.datastore.member.uniref;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.store.job.common.reader.XmlItemReader;
import org.uniprot.store.job.common.util.CommonConstants;

/**
 * @author sahmad
 * @since 23/07/2020
 */
public class UniRefMemberXmlEntryReader extends XmlItemReader<MemberType> {

    public static final String UNIREF_ROOT_ELEMENT = "entry";
    private final OnZeroCountSleeper sleeper;

    public UniRefMemberXmlEntryReader(String filepath) {
        super(filepath, MemberType.class, UNIREF_ROOT_ELEMENT);
        this.sleeper = new OnZeroCountSleeper();
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(CommonConstants.ENTRIES_TO_WRITE_COUNTER, sleeper);
    }

    @Override
    public MemberType read() {
        if (entryIterator.hasNext()) {
            sleeper.increment();
            return entryIterator.next();
        } else {
            return null;
        }
    }
}
