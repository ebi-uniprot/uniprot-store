package org.uniprot.store.indexer.search.precomputed;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

class ValidateAllFieldsExistIT
        extends AbstractValidateAllFieldsExist<PrecomputedAnnotationDocument> {
    @RegisterExtension
    static PrecomputedAnnotationSearchEngine searchEngine = new PrecomputedAnnotationSearchEngine();

    protected AbstractSearchEngine<PrecomputedAnnotationDocument> getSearchEngine() {
        return searchEngine;
    }
}
