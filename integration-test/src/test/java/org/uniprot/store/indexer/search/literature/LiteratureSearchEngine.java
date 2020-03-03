package org.uniprot.store.indexer.search.literature;

import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.factory.UniProtDataType;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.literature.LiteratureDocument;

class LiteratureSearchEngine extends AbstractSearchEngine<LiteratureDocument> {
    private static final String SEARCH_ENGINE_NAME = "literature";

    LiteratureSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected SearchFieldConfig getSearchFieldConfig() {
        return SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.LITERATURE);
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
