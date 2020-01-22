package org.uniprot.store.indexer.search.suggest;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.domain2.UniProtSearchFields;

class SuggestSearchEngine extends AbstractSearchEngine<SuggestDocument> {

    private static final String SEARCH_ENGINE_NAME = "suggest";

    SuggestSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.SUGGEST.getField("id").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "(" + UniProtSearchFields.SUGGEST.getField("id").getName() + ":\"" + entryId + "\")";
    }
}
