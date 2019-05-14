package uk.ac.ebi.uniprot.indexer.search.suggest;

import uk.ac.ebi.uniprot.indexer.search.AbstractSearchEngine;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;
import uk.ac.ebi.uniprot.search.field.SuggestField;

public class SuggestSearchEngine extends AbstractSearchEngine<SuggestDocument> {

    private static final String SEARCH_ENGINE_NAME = "suggest";

    public SuggestSearchEngine() {
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
