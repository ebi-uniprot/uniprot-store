package org.uniprot.store.indexer.publication.common;

/**
 * @author lgonzales
 * @since 13/01/2021
 */
public enum LargeScaleSolrFieldQuery {
    COMPUTATIONAL("citations_with:4_computationally"),
    COMMUNITY("citations_with:5_community"),
    UNIPROT_KB("citations_with:1_uniprotkb");

    private final String solrFieldNameQuery;

    LargeScaleSolrFieldQuery(String solrFieldNameQuery) {
        this.solrFieldNameQuery = solrFieldNameQuery;
    }

    public String getSolrFieldNameQuery() {
        return solrFieldNameQuery;
    }
}
