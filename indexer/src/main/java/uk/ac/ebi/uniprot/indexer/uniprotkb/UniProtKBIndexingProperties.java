package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@ConfigurationProperties(prefix = "uniprotkb.indexing")
public class UniProtKBIndexingProperties {
    private int chunkSize = 1000;
    private int skipLimit = 0;
    private int retryLimit = 10000;
    private int entryIteratorThreads = 2;
    private int entryIteratorQueueSize = 0;
    private int entryIteratorFFQueueSize = 0;
    private String subcellularLocationFile;
    private String accessionGoPubmedFile;
    private String goFile;
    private String goRelationsFile;
    private String uniProtEntryFile;
    private String diseaseFile;
    private String keywordFile;
    private String pathwayFile;
    private String taxonomyFile;

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void setRetryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
    }

    public int getEntryIteratorThreads() {
        return entryIteratorThreads;
    }

    public int getEntryIteratorQueueSize() {
        return entryIteratorQueueSize;
    }

    public int getEntryIteratorFFQueueSize() {
        return entryIteratorFFQueueSize;
    }

    public String getUniProtEntryFile() {
        return uniProtEntryFile;
    }

    public String getKeywordFile() {
        return keywordFile;
    }

    public String getDiseaseFile() {
        return diseaseFile;
    }

    public String getAccessionGoPubmedFile() {
        return accessionGoPubmedFile;
    }

    public String getSubcellularLocationFile() {
        return subcellularLocationFile;
    }

    public void setEntryIteratorThreads(int entryIteratorThreads) {
        this.entryIteratorThreads = entryIteratorThreads;
    }

    public void setEntryIteratorQueueSize(int entryIteratorQueueSize) {
        this.entryIteratorQueueSize = entryIteratorQueueSize;
    }

    public void setEntryIteratorFFQueueSize(int entryIteratorFFQueueSize) {
        this.entryIteratorFFQueueSize = entryIteratorFFQueueSize;
    }

    public void setSubcellularLocationFile(String subcellularLocationFile) {
        this.subcellularLocationFile = subcellularLocationFile;
    }

    public void setAccessionGoPubmedFile(String accessionGoPubmedFile) {
        this.accessionGoPubmedFile = accessionGoPubmedFile;
    }

    public void setUniProtEntryFile(String uniProtEntryFile) {
        this.uniProtEntryFile = uniProtEntryFile;
    }

    public void setDiseaseFile(String diseaseFile) {
        this.diseaseFile = diseaseFile;
    }

    public void setKeywordFile(String keywordFile) {
        this.keywordFile = keywordFile;
    }

    public String getPathwayFile() {
        return pathwayFile;
    }

    public void setPathwayFile(String pathwayFile) {
        this.pathwayFile = pathwayFile;
    }

    public String getTaxonomyFile() {
        return taxonomyFile;
    }

    public void setTaxonomyFile(String taxonomyFile) {
        this.taxonomyFile = taxonomyFile;
    }

    public String getGoRelationsFile() {
        return goRelationsFile;
    }

    public void setGoRelationsFile(String goRelationsFile) {
        this.goRelationsFile = goRelationsFile;
    }

    public String getGoFile() {
        return goFile;
    }

    public void setGoFile(String goFile) {
        this.goFile = goFile;
    }
}
