package org.uniprot.store.indexer.search.genecentric;

import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;

class GeneCentricSearchEngine extends AbstractSearchEngine<GeneCentricDocument> {
    private static final String SEARCH_ENGINE_NAME = "genecentric";

    GeneCentricSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected SearchFieldConfig getSearchFieldConfig() {
        return SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.GENECENTRIC);
    }

    @Override
    protected String identifierField() {
        return getSearchFieldConfig().getSearchFieldItemByName("accession").getFieldName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + getSearchFieldConfig().getSearchFieldItemByName("accession").getFieldName()
                + ":\""
                + entryId
                + "\")";
    }
}
