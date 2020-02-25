package org.uniprot.store.indexer.search.crossref;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.dbxref.CrossRefDocument;

/**
 * This class tests that all cross reference search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<CrossRefDocument> {
    @RegisterExtension static CrossRefSearchEngine searchEngine = new CrossRefSearchEngine();

    protected AbstractSearchEngine<CrossRefDocument> getSearchEngine() {
        return searchEngine;
    }
}
