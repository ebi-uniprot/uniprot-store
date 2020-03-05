package org.uniprot.store.indexer.search.taxonomy;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

/**
 * This class tests that all taxonomy search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<TaxonomyDocument> {
    @RegisterExtension static TaxonomySearchEngine searchEngine = new TaxonomySearchEngine();

    protected AbstractSearchEngine<TaxonomyDocument> getSearchEngine() {
        return searchEngine;
    }
}
