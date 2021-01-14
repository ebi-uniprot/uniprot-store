package org.uniprot.store.indexer.publication.common;

/**
 * @author lgonzales
 * @since 13/01/2021
 */
public enum LargeScaleSolrFieldName {
    COMPUTATIONAL("is_computational_mapped"),
    COMMUNITY("is_community_mapped"),
    UNIPROT_KB("is_uniprotkb_mapped");

    private final String solrFieldName;

    LargeScaleSolrFieldName(String solrFieldName) {
        this.solrFieldName = solrFieldName;
    }

    public String getSolrFieldName() {
        return solrFieldName;
    }
}
