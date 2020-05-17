package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class IndexDataStoreMainTest {

    @Test
    void testIndexDataStoreMainInvalidArgumentIndex() {
        assertThrows(IllegalArgumentException.class, () -> IndexDataStoreMain.main(new String[0]));
    }

    @Test
    void testIndexDataStoreMainInvalidCollection() {
        String[] args = {"invalid", "invalid"};
        assertThrows(IndexDataStoreException.class, () -> IndexDataStoreMain.main(args));
    }
}
