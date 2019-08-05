package org.uniprot.store.job.common.concurrent;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.uniprot.core.util.concurrency.TaskExecutorProperties;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created 28/07/19
 *
 * @author Edd
 */
public class TaskExecutorPropertiesConverter {
    public static ThreadPoolTaskExecutor createThreadPoolTaskExecutor(TaskExecutorProperties properties) {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(properties.getCorePoolSize());
        taskExecutor.setMaxPoolSize(properties.getMaxPoolSize());
        taskExecutor.setQueueCapacity(properties.getQueueCapacity());
        taskExecutor.setKeepAliveSeconds(properties.getKeepAliveSeconds());
        taskExecutor.setAllowCoreThreadTimeOut(properties.isAllowCoreThreadTimeout());
        taskExecutor.setWaitForTasksToCompleteOnShutdown(properties.isWaitForTasksToCompleteOnShutdown());
        taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        taskExecutor.initialize();
        taskExecutor.setThreadNamePrefix(properties.getThreadNamePrefix());
        return taskExecutor;
    }
}
