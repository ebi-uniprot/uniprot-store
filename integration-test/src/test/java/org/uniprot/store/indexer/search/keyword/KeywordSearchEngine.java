package org.uniprot.store.indexer.search.keyword;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.keyword.KeywordDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

class KeywordSearchEngine extends AbstractSearchEngine<KeywordDocument> {
    private static final String SEARCH_ENGINE_NAME = "keyword";

    KeywordSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.KEYWORD.getField("id").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + UniProtSearchFields.KEYWORD.getField("id").getName()
                + ":\""
                + entryId
                + "\")";
    }
}
