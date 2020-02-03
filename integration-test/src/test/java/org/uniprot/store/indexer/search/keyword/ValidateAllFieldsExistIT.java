package org.uniprot.store.indexer.search.keyword;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.keyword.KeywordDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

/**
 * This class tests that all keyword search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<KeywordDocument> {
    @RegisterExtension static KeywordSearchEngine searchEngine = new KeywordSearchEngine();

    protected AbstractSearchEngine<KeywordDocument> getSearchEngine() {
        return searchEngine;
    }

    protected UniProtSearchFields getSearchFieldType() {
        return UniProtSearchFields.KEYWORD;
    }
}
