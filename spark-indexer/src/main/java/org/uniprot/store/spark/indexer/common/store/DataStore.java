package org.uniprot.store.spark.indexer.common.store;

/**
 * @author lgonzales
 * @since 27/04/2020
 */
public enum DataStore {
    UNIPROT("uniprot"),
    UNIREF_LIGHT("uniref-light"),
    UNIREF_MEMBER("uniref-member"),
    UNIPARC_LIGHT("uniparc-light"),
    UNIPARC_CROSS_REFERENCE("uniparc-cross-reference"),
    GOOGLE_PROTNLM("google-protnlm");

    private final String name;

    DataStore(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
