package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class IndexHDFSDocumentsInSolrMainTest {

    @Test
    void testIIndexHDFSDocumentsInSolrMainInvalidArgumentIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> IndexHDFSDocumentsInSolrMain.main(new String[0]));
    }

    @Test
    void testIndexHDFSDocumentsInSolrMainInvalidCollection() {
        String[] args = {"invalid", "invalid"};
        assertThrows(SolrIndexException.class, () -> IndexHDFSDocumentsInSolrMain.main(args));
    }
}
