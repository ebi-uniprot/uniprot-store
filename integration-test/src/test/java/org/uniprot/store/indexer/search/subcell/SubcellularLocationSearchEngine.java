package org.uniprot.store.indexer.search.subcell;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

class SubcellularLocationSearchEngine extends AbstractSearchEngine<SubcellularLocationDocument> {
    private static final String SEARCH_ENGINE_NAME = "subcellularlocation";

    SubcellularLocationSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.SUBCELL.getField("id").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "(" + UniProtSearchFields.SUBCELL.getField("id").getName() + ":\"" + entryId + "\")";
    }
}
