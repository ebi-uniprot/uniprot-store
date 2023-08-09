package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.*;

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
}
