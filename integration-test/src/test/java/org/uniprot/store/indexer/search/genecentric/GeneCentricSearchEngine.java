package org.uniprot.store.indexer.search.genecentric;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

class GeneCentricSearchEngine extends AbstractSearchEngine<GeneCentricDocument> {
    private static final String SEARCH_ENGINE_NAME = "genecentric";

    GeneCentricSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.GENECENTRIC.getField("accession").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + UniProtSearchFields.GENECENTRIC.getField("accession").getName()
                + ":\""
                + entryId
                + "\")";
    }
}
