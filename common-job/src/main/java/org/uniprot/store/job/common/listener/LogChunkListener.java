package org.uniprot.store.job.common.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.util.List;

/**
 * @author lgonzales
 */
@Slf4j
public class LogChunkListener implements ChunkListener {

    @Override
    public void beforeChunk(ChunkContext chunkContext) {
        log.trace("before chunk");
    }

    @Override
    public void afterChunk(ChunkContext chunkContext) {
        String stepName = chunkContext.getStepContext().getStepName();
        int writeCount = chunkContext.getStepContext().getStepExecution().getWriteCount();
        log.info("Executed chunk for {}: {}", stepName, writeCount);
    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        String stepName = chunkContext.getStepContext().getStepName();
        String status = chunkContext.getStepContext().getStepExecution().getStatus().name();
        log.warn("Failed to Executed chunk for step: {} and with status: {}", stepName, status);
        List<Throwable> errors = chunkContext.getStepContext().getStepExecution().getFailureExceptions();
        errors.forEach(throwable -> log.error("Chunk Error: ", throwable));
    }

}
