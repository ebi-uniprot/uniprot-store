package org.uniprot.store.datastore.uniprotkb.config;

import static org.uniprot.store.job.common.concurrent.TaskExecutorPropertiesConverter.createThreadPoolTaskExecutor;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.uniprot.core.util.concurrency.TaskExecutorProperties;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

import lombok.extern.slf4j.Slf4j;

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
    private final UniProtKBStoreProperties uniProtKBStoreProperties;

    @Autowired
    public AsyncConfig(UniProtKBStoreProperties uniProtKBStoreProperties) {
        this.uniProtKBStoreProperties = uniProtKBStoreProperties;
    }

    /**
     * Used by {@link ItemRetryWriter#write(List)}.
     *
     * @return the task executor used when writing items
     */
    @Bean(ItemRetryWriter.ITEM_WRITER_TASK_EXECUTOR)
    public ThreadPoolTaskExecutor itemWriterTaskExecutor() {
        TaskExecutorProperties taskExecutorProperties =
                uniProtKBStoreProperties.getItemWriterTaskExecutor();
        ThreadPoolTaskExecutor taskExecutor = createThreadPoolTaskExecutor(taskExecutorProperties);
        log.info("Using Item Writer task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }
}
