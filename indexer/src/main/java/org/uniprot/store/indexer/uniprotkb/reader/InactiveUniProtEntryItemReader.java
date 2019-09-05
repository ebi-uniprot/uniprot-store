package org.uniprot.store.indexer.uniprotkb.reader;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveEntryIterator;
import org.uniprot.store.indexer.uniprotkb.model.InactiveEntryDocumentPair;
import org.uniprot.store.job.common.util.CommonConstants;

/**
 *
 * @author jluo
 * @date: 5 Sep 2019
 *
*/

public class InactiveUniProtEntryItemReader implements ItemReader<InactiveEntryDocumentPair> {
	private InactiveEntryIterator  entryIterator;
	private final OnZeroCountSleeper sleeper;
	public InactiveUniProtEntryItemReader(InactiveEntryIterator  entryIterator) {
		this.entryIterator = entryIterator;
        this.sleeper = new OnZeroCountSleeper();
	}
	@Override
	public InactiveEntryDocumentPair read()
			throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		if (entryIterator.hasNext()) {
			   sleeper.increment();
            return new InactiveEntryDocumentPair(entryIterator.next());
        } else {
        	entryIterator.close();
            return null;
        }
	}
	 @BeforeStep
	    public void setStepExecution(final StepExecution stepExecution) {
	        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
	        executionContext.put(CommonConstants.ENTRIES_TO_WRITE_COUNTER, sleeper);
	    }
}

