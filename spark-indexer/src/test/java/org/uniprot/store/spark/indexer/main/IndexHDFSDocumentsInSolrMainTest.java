package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class IndexHDFSDocumentsInSolrMainTest {

    @Test
    void testIndexHDFSDocumentsInSolrMainInvalidArgumentIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> IndexHDFSDocumentsInSolrMain.main(new String[0]));
    }

    @Test
    void testIndexHDFSDocumentsInSolrMainInvalidCollection() {
        String[] args = {"invalid", "invalid", "invalid"};
        assertThrows(SolrIndexException.class, () -> IndexHDFSDocumentsInSolrMain.main(args));
    }

    @Test
    void testIndexHDFSDocumentsInSolrMainThrowExceptions() {
        // valid arguments, but it will fail because we do not have the serialized document files.
        String[] args = {"2020_04", "uniparc", SPARK_LOCAL_MASTER};
        assertThrows(SolrIndexException.class, () -> IndexHDFSDocumentsInSolrMain.main(args));
    }
}
