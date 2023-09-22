package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class WriteIndexDocumentsToHPSMainTest {

    @Test
    void testWriteIndexDocumentsToHPSMainInvalidArgumentIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> WriteIndexDocumentsToHPSMain.main(new String[0]));
    }

    @Test
    void testWriteIndexDocumentsToHPSMainInvalidCollection() {
        String[] args = {"invalid", "invalid", "invalid"};
        assertThrows(
                IndexHPSDocumentsException.class, () -> WriteIndexDocumentsToHPSMain.main(args));
    }

    @Test
    void testWriteIndexDocumentsToHPSMain() {
        String[] args = {"2023_04", "uniprot", SPARK_LOCAL_MASTER};
        assertThrows(
                IndexHPSDocumentsException.class, () -> WriteIndexDocumentsToHPSMain.main(args));
    }
}
