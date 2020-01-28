package org.uniprot.store.indexer.search.uniprot;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;
import org.uniprot.store.search.field.UniProtSearchFields;

/**
 * This class tests that all UniProt search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<UniProtEntry> {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    protected AbstractSearchEngine<UniProtEntry> getSearchEngine() {
        return searchEngine;
    }

    protected UniProtSearchFields getSearchFieldType() {
        return UniProtSearchFields.UNIPROTKB;
    }
}
