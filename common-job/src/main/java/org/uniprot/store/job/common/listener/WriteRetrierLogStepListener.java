/*
 * Created by sahmad on 29/01/19 19:28
 * UniProt Consortium.
 * Copyright (c) 2002-2019.
 *
 */

package org.uniprot.store.job.common.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ExecutionContext;
import org.uniprot.core.util.Utils;
import org.uniprot.core.util.concurrency.OnZeroCountSleeper;
import org.uniprot.store.job.common.util.CommonConstants;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WriteRetrierLogStepListener implements StepExecutionListener {
    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Step '{}' starting.", stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        if (executionContext.containsKey(CommonConstants.ENTRIES_TO_WRITE_COUNTER)) {
            OnZeroCountSleeper sleeper =
                    (OnZeroCountSleeper)
                            executionContext.get(CommonConstants.ENTRIES_TO_WRITE_COUNTER);
            if (Utils.notNull(sleeper)) {
                sleeper.sleepUntilZero();
            }
        }

        AtomicInteger failedCountAtomicInteger =
                (AtomicInteger) executionContext.get(CommonConstants.FAILED_ENTRIES_COUNT_KEY);
        int failedCount = -1;
        if (failedCountAtomicInteger != null) {
            failedCount = failedCountAtomicInteger.get();
            if (failedCount > 0) {
                stepExecution.setExitStatus(ExitStatus.FAILED);
                stepExecution.setStatus(BatchStatus.FAILED);
                stepExecution.getJobExecution().setStatus(BatchStatus.FAILED);
                stepExecution.getJobExecution().setExitStatus(ExitStatus.FAILED);
            }
        }

        AtomicInteger writtenCountAtomicInteger =
                (AtomicInteger) executionContext.get(CommonConstants.WRITTEN_ENTRIES_COUNT_KEY);
        int writtenCount = -1;
        if (writtenCountAtomicInteger != null) {
            writtenCount = writtenCountAtomicInteger.get();
        }

        log.info("=====================================================");
        log.info("                   Step Statistics                   ");
        log.info("Step name      : {}", stepExecution.getStepName());
        log.info("Exit status    : {}", stepExecution.getExitStatus().getExitCode());
        log.info("Read count     : {}", stepExecution.getReadCount());
        log.info("Write count    : {}", writtenCount);
        log.info("Failed count   : {}", failedCount);
        log.info("=====================================================");
        return stepExecution.getExitStatus();
    }
}
