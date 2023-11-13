package org.uniprot.store.spark.indexer.main;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

class SolrIndexValidatorMainTest {

    @Test
    void errorWithInvalidArguments() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SolrIndexValidatorMain.main(new String[2]));
    }

    @Test
    void testIndexHPSDocumentsInSolrMainInvalidCollection() {
        String[] args = {"invalid", "invalid", "invalid"};
        assertThrows(SparkIndexException.class, () -> SolrIndexValidatorMain.main(args));
    }

    @Test
    void testIndexHPSDocumentsInSolrMainThrowExceptions() {
        // valid arguments, but it will fail because we do not have the serialized document files.
        String[] args = {"2020_04", "uniparc", SPARK_LOCAL_MASTER};
        assertThrows(SparkIndexException.class, () -> SolrIndexValidatorMain.main(args));
    }

}