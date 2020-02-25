package org.uniprot.store.indexer.search.suggest;

import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.factory.UniProtDataType;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.suggest.SuggestDocument;

class SuggestSearchEngine extends AbstractSearchEngine<SuggestDocument> {

    private static final String SEARCH_ENGINE_NAME = "suggest";

    SuggestSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected SearchFieldConfig getSearchFieldConfig() {
        return SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.suggest);
    }

    @Override
    protected String identifierField() {
        return getSearchFieldConfig().getSearchFieldItemByName("id").getFieldName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + getSearchFieldConfig().getSearchFieldItemByName("id").getFieldName()
                + ":\""
                + entryId
                + "\")";
    }
}
