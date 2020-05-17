package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.IndexHDFSDocumentsException;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class WriteIndexDocumentsToHDFSMainTest {

    @Test
    void testWriteIndexDocumentsToHDFSMainInvalidArgumentIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> WriteIndexDocumentsToHDFSMain.main(new String[0]));
    }

    @Test
    void testWriteIndexDocumentsToHDFSMainInvalidCollection() {
        String[] args = {"invalid", "invalid"};
        assertThrows(
                IndexHDFSDocumentsException.class, () -> WriteIndexDocumentsToHDFSMain.main(args));
    }
}
