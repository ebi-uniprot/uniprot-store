package uk.ac.ebi.uniprot.indexer.uniprotkb;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
public class UniProtKBIndexingProperties {
    // TODO: 10/04/19 all external configuration properties in here. make sure you add getters and setters

    private int chunkSize = 1000;
    private int skipLimit = 0;
    private int retryLimit = 10000;

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
}
