package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

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
        String[] args = {"invalid", "invalid", "invalid", "read"};
        assertThrows(IndexDataStoreException.class, () -> IndexDataStoreMain.main(args));
    }

    @Test
    void testIndexDataStoreMainInvalidTaxDb() {
        String[] args = {"2020_04", "uniparc", SPARK_LOCAL_MASTER, "invalid"};
        assertThrows(IllegalArgumentException.class, () -> IndexDataStoreMain.main(args));
    }
}
