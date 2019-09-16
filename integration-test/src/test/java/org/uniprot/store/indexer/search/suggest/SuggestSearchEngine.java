package org.uniprot.store.indexer.search.suggest;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.field.SuggestField;

class SuggestSearchEngine extends AbstractSearchEngine<SuggestDocument> {

    private static final String SEARCH_ENGINE_NAME = "suggest";

    SuggestSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected Enum identifierField() {
        return SuggestField.Search.id;
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "(" + SuggestField.Search.id + ":\"" + entryId + "\")";
    }
}
