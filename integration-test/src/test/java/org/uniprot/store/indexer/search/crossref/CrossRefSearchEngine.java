package org.uniprot.store.indexer.search.crossref;

import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.dbxref.CrossRefDocument;

class CrossRefSearchEngine extends AbstractSearchEngine<CrossRefDocument> {
    private static final String SEARCH_ENGINE_NAME = "crossref";

    CrossRefSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected SearchFieldConfig getSearchFieldConfig() {
        return SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.CROSSREF);
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
