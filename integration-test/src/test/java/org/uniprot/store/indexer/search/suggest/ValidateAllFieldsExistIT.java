package org.uniprot.store.indexer.search.suggest;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

/**
 * This class tests that all Proteome search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<SuggestDocument> {
    @RegisterExtension static SuggestSearchEngine searchEngine = new SuggestSearchEngine();

    protected AbstractSearchEngine<SuggestDocument> getSearchEngine() {
        return searchEngine;
    }

    protected UniProtSearchFields getSearchFieldType() {
        return UniProtSearchFields.SUGGEST;
    }
}
