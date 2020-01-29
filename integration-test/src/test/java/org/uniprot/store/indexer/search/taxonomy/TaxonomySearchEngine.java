package org.uniprot.store.indexer.search.taxonomy;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

class TaxonomySearchEngine extends AbstractSearchEngine<TaxonomyDocument> {
    private static final String SEARCH_ENGINE_NAME = "taxonomy";

    TaxonomySearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.TAXONOMY.getField("id").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + UniProtSearchFields.TAXONOMY.getField("id").getName()
                + ":\""
                + entryId
                + "\")";
    }
}
