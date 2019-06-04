package uk.ac.ebi.uniprot.indexer.common.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;

import java.util.concurrent.TimeUnit;

@Slf4j
public class LogJobListener implements JobExecutionListener {
    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Job {} starting ...", Constants.SUPPORTING_DATA_INDEX_JOB);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("Job {} completed.", Constants.SUPPORTING_DATA_INDEX_JOB);

        long durationMillis = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();

        String duration =
                String.format("%d hrs, %d min, %d sec",
                              TimeUnit.MILLISECONDS.toHours(durationMillis),
                              TimeUnit.MILLISECONDS.toMinutes(durationMillis) -
                                      TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(durationMillis)),
                              TimeUnit.MILLISECONDS.toSeconds(durationMillis) -
                                      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(durationMillis))
                );

        log.info("=====================================================");
        log.info("              {} Job Statistics                 ", jobExecution.getJobInstance().getJobName());
        log.info("Exit status   : {}", jobExecution.getExitStatus().getExitCode());
        log.info("Start time    : {}", jobExecution.getStartTime());
        log.info("End time      : {}", jobExecution.getEndTime());
        log.info("Duration      : {}", duration);

        long skipCount = 0L;
        long readSkips = 0L;
        long writeSkips = 0L;
        long processingSkips = 0L;
        long readCount = 0L;
        long writeCount = 0L;

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            readSkips += stepExecution.getReadSkipCount();
            writeSkips += stepExecution.getWriteSkipCount();
            processingSkips += stepExecution.getProcessSkipCount();
            readCount += stepExecution.getReadCount();
            writeCount += stepExecution.getWriteCount();
            skipCount += stepExecution.getSkipCount();
        }

        log.info("Read count    : {}", readCount);
        log.info("Write count   : {}", writeCount);
        log.info("Skip count    : {} ({} read, {} processing and {} write)", skipCount, readSkips, processingSkips, writeSkips);
    }
}
