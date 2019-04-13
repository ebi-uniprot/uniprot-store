package uk.ac.ebi.uniprot.indexer.uniprotkb;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Data
@ConfigurationProperties(prefix = "uniprotkb.indexing")
public class UniProtKBIndexingProperties {
    private int chunkSize = 1000;
    private int skipLimit = 0;
    private int retryLimit = 10000;
    private int entryIteratorThreads = 2;
    private int entryIteratorQueueSize = 50;
    private int entryIteratorFFQueueSize = 5000;
    private String subcellularLocationFile;
    private String accessionGoPubmedFile;
    private String goDir;
    private String uniProtEntryFile;
    private String diseaseFile;
    private String keywordFile;
    private String pathwayFile;
    private String taxonomyFile;
}
