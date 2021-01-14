package org.uniprot.store.indexer.publication.common;

import java.util.List;
import java.util.Set;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.uniprot.store.indexer.common.utils.Constants;

/**
 * @author lgonzales
 * @since 13/01/2021
 */
public class LargeScaleWriter implements ItemWriter<Set<String>> {

    private StepExecution stepExecution;

    @Override
    public void write(List<? extends Set<String>> items) throws Exception {
        if (this.stepExecution != null) { // null if being called from unit test
            JobExecution jobExecution = this.stepExecution.getJobExecution();
            ExecutionContext executionContext = jobExecution.getExecutionContext();
            executionContext.put(Constants.PUBLICATION_LARGE_SCALE_KEY, items);
        }
    }

    @BeforeStep // set the stepExecution to pass data from this step to another step.
    // See above executionContext.put() call
    public void setStepExecution(final StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
