package org.uniprot.store.datastore.light.uniref.config;

import static org.uniprot.store.job.common.concurrent.TaskExecutorPropertiesConverter.createThreadPoolTaskExecutor;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.uniprot.core.util.concurrency.TaskExecutorProperties;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
@Configuration
@EnableAsync
@Import(UniRefLightStoreConfig.class)
@Slf4j
public class UniRefLightAsnycConfig {
    private final UniRefLightStoreProperties unirefLightStoreProperties;

    @Autowired
    public UniRefLightAsnycConfig(UniRefLightStoreProperties unirefLightStoreProperties) {
        this.unirefLightStoreProperties = unirefLightStoreProperties;
    }

    /**
     * Used by {@link ItemRetryWriter#write(List)}.
     *
     * @return the task executor used when writing items
     */
    @Bean(ItemRetryWriter.ITEM_WRITER_TASK_EXECUTOR)
    public ThreadPoolTaskExecutor itemWriterTaskExecutor() {
        TaskExecutorProperties taskExecutorProperties =
                unirefLightStoreProperties.getItemWriterTaskExecutor();
        ThreadPoolTaskExecutor taskExecutor = createThreadPoolTaskExecutor(taskExecutorProperties);
        log.info("Using Item Writer task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }
}
