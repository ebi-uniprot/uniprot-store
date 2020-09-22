package org.uniprot.store.datastore.light.uniref.config;

import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.uniprot.core.util.concurrency.TaskExecutorProperties;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
@Data
@ConfigurationProperties(prefix = "uniref.light.store")
public class UniRefLightStoreProperties {
    private int chunkSize = 200;
    private int writeRetryLimit = 3;
    private int writeRetryBackOffFromMillis = 50;
    private int writeRetryBackOffToMillis = 3000;
    private int logRateInterval = 10000;
    private String xmlFilePath;
    private TaskExecutorProperties itemWriterTaskExecutor = new TaskExecutorProperties();
}
