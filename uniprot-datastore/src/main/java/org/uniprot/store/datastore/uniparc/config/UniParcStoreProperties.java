package org.uniprot.store.datastore.uniparc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.uniprot.core.util.concurrency.TaskExecutorProperties;

import lombok.Data;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
@Data
@ConfigurationProperties(prefix = "uniparc.store")
public class UniParcStoreProperties {
    private int chunkSize = 200;
    private int writeRetryLimit = 3;
    private int writeRetryBackOffFromMillis = 50;
    private int writeRetryBackOffToMillis = 3000;
    private int logRateInterval = 10000;
    private String xmlFilePath;
    private String taxonomyFilePath;
    private TaskExecutorProperties itemWriterTaskExecutor = new TaskExecutorProperties();
}
