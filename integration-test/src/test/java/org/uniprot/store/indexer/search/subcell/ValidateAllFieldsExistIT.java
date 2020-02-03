package org.uniprot.store.indexer.search.subcell;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;
import org.uniprot.store.search.field.UniProtSearchFields;

/**
 * This class tests that all subcellular location search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<SubcellularLocationDocument> {
    @RegisterExtension
    static SubcellularLocationSearchEngine searchEngine = new SubcellularLocationSearchEngine();

    protected AbstractSearchEngine<SubcellularLocationDocument> getSearchEngine() {
        return searchEngine;
    }

    protected UniProtSearchFields getSearchFieldType() {
        return UniProtSearchFields.SUBCELL;
    }
}
