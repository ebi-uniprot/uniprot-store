package uk.ac.ebi.uniprot.datastore.uniprotkb.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import uk.ac.ebi.uniprot.common.concurrency.TaskExecutorProperties;
import uk.ac.ebi.uniprot.job.common.writer.ItemRetryWriter;

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
    private final UniProtKBStoreProperties uniProtKBStoreProperties;

    @Autowired
    public AsyncConfig(UniProtKBStoreProperties indexingProperties) {
        this.uniProtKBStoreProperties = indexingProperties;
    }

    /**
     * Used by {@link ItemRetryWriter#write(List)}.
     * @return the task executor used when writing items
     */
    @Bean(ItemRetryWriter.ITEM_WRITER_TASK_EXECUTOR)
    public ThreadPoolTaskExecutor itemWriterTaskExecutor() {
        TaskExecutorProperties taskExecutorProperties = uniProtKBStoreProperties
                .getItemWriterTaskExecutor();
        ThreadPoolTaskExecutor taskExecutor = createThreadPoolTaskExecutor(taskExecutorProperties);
        log.info("Using Item Writer task executor: {}", taskExecutorProperties);
        return taskExecutor;
    }
}
