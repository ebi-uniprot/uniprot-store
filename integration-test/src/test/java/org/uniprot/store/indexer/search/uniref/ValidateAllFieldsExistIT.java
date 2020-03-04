package org.uniprot.store.indexer.search.uniref;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;

/**
 * This class tests that all UniRef search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT
        extends AbstractValidateAllFieldsExist<org.uniprot.core.xml.jaxb.uniref.Entry> {
    @RegisterExtension static UniRefSearchEngine searchEngine = new UniRefSearchEngine();

    protected AbstractSearchEngine<org.uniprot.core.xml.jaxb.uniref.Entry> getSearchEngine() {
        return searchEngine;
    }
}
