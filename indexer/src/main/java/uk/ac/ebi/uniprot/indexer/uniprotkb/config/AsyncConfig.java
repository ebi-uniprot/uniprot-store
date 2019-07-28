package uk.ac.ebi.uniprot.indexer.uniprotkb.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import uk.ac.ebi.uniprot.common.concurrency.TaskExecutorProperties;
import uk.ac.ebi.uniprot.indexer.common.writer.EntryDocumentPairRetryWriter;

import java.util.List;

import static uk.ac.ebi.uniprot.job.common.concurrent.TaskExecutorPropertiesConverter.createThreadPoolTaskExecutor;

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
        ThreadPoolTaskExecutor taskExecutor = createThreadPoolTaskExecutor(taskExecutorProperties);
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
        ThreadPoolTaskExecutor taskExecutor = createThreadPoolTaskExecutor(taskExecutorProperties);
        log.info("Using Item Writer task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }
}
