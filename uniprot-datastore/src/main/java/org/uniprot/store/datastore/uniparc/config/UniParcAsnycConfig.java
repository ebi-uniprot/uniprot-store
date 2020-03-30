package org.uniprot.store.datastore.uniparc.config;

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
 * @since 2020-03-03
 */
@Configuration
@EnableAsync
@Import(UniParcStoreConfig.class)
@Slf4j
public class UniParcAsnycConfig {

    private final UniParcStoreProperties uniParcStoreProperties;

    @Autowired
    public UniParcAsnycConfig(UniParcStoreProperties uniParcStoreProperties) {
        this.uniParcStoreProperties = uniParcStoreProperties;
    }

    /**
     * Used by {@link ItemRetryWriter#write(List)}.
     *
     * @return the task executor used when writing items
     */
    @Bean(ItemRetryWriter.ITEM_WRITER_TASK_EXECUTOR)
    public ThreadPoolTaskExecutor itemWriterTaskExecutor() {
        TaskExecutorProperties taskExecutorProperties =
                uniParcStoreProperties.getItemWriterTaskExecutor();
        ThreadPoolTaskExecutor taskExecutor = createThreadPoolTaskExecutor(taskExecutorProperties);
        log.info("Using Item Writer task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }
}
