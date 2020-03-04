package org.uniprot.store.indexer.search.proteome;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.search.AbstractValidateAllFieldsExist;

/**
 * This class tests that all Proteome search fields can be queried against.
 *
 * <p>Created 28/01/2020
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT
        extends AbstractValidateAllFieldsExist<org.uniprot.core.xml.jaxb.proteome.Proteome> {
    @RegisterExtension static ProteomeSearchEngine searchEngine = new ProteomeSearchEngine();

    protected AbstractSearchEngine<org.uniprot.core.xml.jaxb.proteome.Proteome> getSearchEngine() {
        return searchEngine;
    }
}
