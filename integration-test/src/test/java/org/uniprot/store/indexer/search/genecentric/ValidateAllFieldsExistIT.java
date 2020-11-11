package org.uniprot.store.indexer.search.genecentric;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;

/**
 * This class tests that all gene centric search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<GeneCentricDocument> {
    @RegisterExtension static GeneCentricSearchEngine searchEngine = new GeneCentricSearchEngine();

    protected AbstractSearchEngine<GeneCentricDocument> getSearchEngine() {
        return searchEngine;
    }
}
