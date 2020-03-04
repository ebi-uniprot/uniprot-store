package org.uniprot.store.indexer.search.literature;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * This class tests that all literature search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<LiteratureDocument> {
    @RegisterExtension static LiteratureSearchEngine searchEngine = new LiteratureSearchEngine();

    protected AbstractSearchEngine<LiteratureDocument> getSearchEngine() {
        return searchEngine;
    }
}
