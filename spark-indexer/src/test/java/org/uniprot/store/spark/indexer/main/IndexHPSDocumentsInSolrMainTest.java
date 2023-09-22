package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class IndexHPSDocumentsInSolrMainTest {

    @Test
    void testIndexHPSDocumentsInSolrMainInvalidArgumentIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> IndexHPSDocumentsInSolrMain.main(new String[0]));
    }

    @Test
    void testIndexHPSDocumentsInSolrMainInvalidCollection() {
        String[] args = {"invalid", "invalid", "invalid"};
        assertThrows(SolrIndexException.class, () -> IndexHPSDocumentsInSolrMain.main(args));
    }

    @Test
    void testIndexHPSDocumentsInSolrMainThrowExceptions() {
        // valid arguments, but it will fail because we do not have the serialized document files.
        String[] args = {"2020_04", "uniparc", SPARK_LOCAL_MASTER};
        assertThrows(SolrIndexException.class, () -> IndexHPSDocumentsInSolrMain.main(args));
    }
}
