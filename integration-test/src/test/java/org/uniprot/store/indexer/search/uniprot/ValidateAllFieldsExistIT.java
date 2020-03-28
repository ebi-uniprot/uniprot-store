package org.uniprot.store.indexer.search.uniprot;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;

/**
 * This class tests that all UniProt search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT extends AbstractValidateAllFieldsExist<UniProtKBEntry> {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    protected AbstractSearchEngine<UniProtKBEntry> getSearchEngine() {
        return searchEngine;
    }
}
