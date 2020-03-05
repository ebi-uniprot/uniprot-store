package org.uniprot.store.indexer.search.disease;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.disease.DiseaseDocument;

/**
 * This class tests that all disease search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<DiseaseDocument> {
    @RegisterExtension static DiseaseSearchEngine searchEngine = new DiseaseSearchEngine();

    protected AbstractSearchEngine<DiseaseDocument> getSearchEngine() {
        return searchEngine;
    }
}
