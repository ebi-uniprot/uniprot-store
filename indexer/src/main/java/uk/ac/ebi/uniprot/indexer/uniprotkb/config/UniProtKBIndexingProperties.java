package uk.ac.ebi.uniprot.indexer.uniprotkb.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Represents configuration properties required for the indexing of UniProtKB data.
 * <p>
 * Created 10/04/19
 *
 * @author Edd
 */
@Data
@ConfigurationProperties(prefix = "uniprotkb.indexing")
public class UniProtKBIndexingProperties {
    private int chunkSize = 1000;
    private int writeRetryLimit = 100;
    private int writeRetryBackOffFromMillis = 50;
    private int writeRetryBackOffToMillis = 3000;
    private int entryIteratorThreads = 2;
    private int entryIteratorQueueSize = 50;
    private int entryIteratorFFQueueSize = 5000;
    private int uniProtKBLogRateInterval = 100000;
    private int suggestionLogRateInterval = 5000;
    private String subcellularLocationFile;
    private String accessionGoPubmedFile;
    private String goDir;
    private String uniProtEntryFile;
    private String diseaseFile;
    private String keywordFile;
    private String pathwayFile;
    private String taxonomyFile;
    private String chebiFile;
    private String ecDir;
}
