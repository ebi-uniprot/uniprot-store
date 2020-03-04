package org.uniprot.store.indexer.search.uniparc;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;

/**
 * This class tests that all UniParc search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT
        extends AbstractValidateAllFieldsExist<org.uniprot.core.xml.jaxb.uniparc.Entry> {
    @RegisterExtension static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    protected AbstractSearchEngine<org.uniprot.core.xml.jaxb.uniparc.Entry> getSearchEngine() {
        return searchEngine;
    }
}
