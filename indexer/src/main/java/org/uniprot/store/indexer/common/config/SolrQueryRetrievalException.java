package org.uniprot.store.indexer.common.config;

/**
 * Represents a problem that occurred from processing a {@link
 * org.apache.solr.client.solrj.SolrQuery} from Solr.
 *
 * <p>Created 15/05/2020
 *
 * @author Edd
 */
public class SolrQueryRetrievalException extends RuntimeException {
    public SolrQueryRetrievalException(String message, Throwable cause) {
        super(message, cause);
    }
}
