/*
 * Created by sahmad on 29/01/19 19:28
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package uk.ac.ebi.uniprot.indexer.common.listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
@Slf4j
public class LogStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Step '{}' starting.", stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("=====================================================");
        log.info("                   Step Statistics                   ");
        log.info("Step name     : {}", stepExecution.getStepName());
        log.info("Exit status   : {}", stepExecution.getExitStatus().getExitCode());
        log.info("Read count    : {}", stepExecution.getReadCount());
        log.info("Write count   : {}", stepExecution.getWriteCount());
        log.info("Skip count    : {} ({} read / {} processing /{} write)", stepExecution.getSkipCount(),
                stepExecution.getReadSkipCount(), stepExecution.getProcessSkipCount(),
                stepExecution.getWriteSkipCount());
        log.info("=====================================================");
        return stepExecution.getExitStatus();
    }
}
