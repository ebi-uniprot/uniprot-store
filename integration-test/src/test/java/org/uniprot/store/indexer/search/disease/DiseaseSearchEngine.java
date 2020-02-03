package org.uniprot.store.indexer.search.disease;

import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.disease.DiseaseDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

class DiseaseSearchEngine extends AbstractSearchEngine<DiseaseDocument> {
    private static final String SEARCH_ENGINE_NAME = "disease";

    DiseaseSearchEngine() {
        super(SEARCH_ENGINE_NAME, identityConverter -> identityConverter);
    }

    @Override
    protected String identifierField() {
        return UniProtSearchFields.DISEASE.getField("accession").getName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "("
                + UniProtSearchFields.DISEASE.getField("accession").getName()
                + ":\""
                + entryId
                + "\")";
    }
}
