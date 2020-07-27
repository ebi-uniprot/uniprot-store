package org.uniprot.store.datastore.member.uniref.config;

import lombok.Data;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.uniprot.core.util.concurrency.TaskExecutorProperties;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Data
@ConfigurationProperties(prefix = "uniref.member.store")
public class UniRefMemberStoreProperties {
    private int chunkSize = 200;
    private int writeRetryLimit = 3;
    private int writeRetryBackOffFromMillis = 50;
    private int writeRetryBackOffToMillis = 3000;
    private int logRateInterval = 10000;
    private String uniref100XmlFilePath;
    private String uniref90XmlFilePath;
    private String uniref50XmlFilePath;
    private TaskExecutorProperties itemWriterTaskExecutor = new TaskExecutorProperties();
}
