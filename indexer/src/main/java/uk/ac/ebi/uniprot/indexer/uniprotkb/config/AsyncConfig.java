package uk.ac.ebi.uniprot.indexer.uniprotkb.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import uk.ac.ebi.uniprot.indexer.common.concurrency.TaskExecutorProperties;
import uk.ac.ebi.uniprot.indexer.common.writer.EntryDocumentPairRetryWriter;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created 11/07/19
 *
 * @author Edd
 */
@Configuration
@EnableAsync
@Import(UniProtKBConfig.class)
@Slf4j
public class AsyncConfig {
    private final UniProtKBIndexingProperties uniProtKBIndexingProperties;

    @Autowired
    public AsyncConfig(UniProtKBIndexingProperties indexingProperties) {
        this.uniProtKBIndexingProperties = indexingProperties;
    }

    @Bean("itemProcessorTaskExecutor")
    public ThreadPoolTaskExecutor itemProcessorTaskExecutor() {
        TaskExecutorProperties taskExecutorProperties = uniProtKBIndexingProperties
                .getItemProcessorTaskExecutor();
        ThreadPoolTaskExecutor taskExecutor = createTaskExecutor(taskExecutorProperties);
        log.info("Using Item Processor task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }

    /**
     * Used by {@link EntryDocumentPairRetryWriter#write(List)}.
     * @return the task executor used when writing items
     */
    @Bean(EntryDocumentPairRetryWriter.ITEM_WRITER_TASK_EXECUTOR)
    public ThreadPoolTaskExecutor itemWriterTaskExecutor() {
        TaskExecutorProperties taskExecutorProperties = uniProtKBIndexingProperties
                .getItemWriterTaskExecutor();
        ThreadPoolTaskExecutor taskExecutor = createTaskExecutor(taskExecutorProperties);
        log.info("Using Item Writer task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }

    private ThreadPoolTaskExecutor createTaskExecutor(TaskExecutorProperties taskExecutorProperties) {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(taskExecutorProperties.getCorePoolSize());
        taskExecutor.setMaxPoolSize(taskExecutorProperties.getMaxPoolSize());
        taskExecutor.setQueueCapacity(taskExecutorProperties.getQueueCapacity());
        taskExecutor.setKeepAliveSeconds(taskExecutorProperties.getKeepAliveSeconds());
        taskExecutor.setAllowCoreThreadTimeOut(taskExecutorProperties.isAllowCoreThreadTimeout());
        taskExecutor.setWaitForTasksToCompleteOnShutdown(taskExecutorProperties.isWaitForTasksToCompleteOnShutdown());
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();
        taskExecutor.setThreadNamePrefix(taskExecutorProperties.getThreadNamePrefix());
        return taskExecutor;
    }
}
