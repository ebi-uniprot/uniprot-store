package org.uniprot.store.indexer.search.literature;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

class LiteratureSearchEngine extends AbstractSearchEngine<LiteratureDocument> {
    private static final String SEARCH_ENGINE_NAME = "literature";

    LiteratureSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.LITERATURE.getField("id").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + UniProtSearchFields.LITERATURE.getField("id").getName()
                + ":\""
                + entryId
                + "\")";
    }
}
