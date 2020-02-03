package org.uniprot.store.indexer.search.crossref;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.dbxref.CrossRefDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

class CrossRefSearchEngine extends AbstractSearchEngine<CrossRefDocument> {
    private static final String SEARCH_ENGINE_NAME = "crossref";

    CrossRefSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.SUGGEST.getField("accession").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + UniProtSearchFields.CROSSREF.getField("accession").getName()
                + ":\""
                + entryId
                + "\")";
    }
}
