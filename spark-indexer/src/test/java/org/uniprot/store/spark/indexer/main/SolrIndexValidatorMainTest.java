package org.uniprot.store.spark.indexer.main;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

class SolrIndexValidatorMainTest {

    @Test
    void errorWithInvalidArguments() {
        assertThrows(
                IllegalArgumentException.class, () -> SolrIndexValidatorMain.main(new String[2]));
    }

    @Test
    void testSolrIndexValidatorMainInvalidCollection() {
        String[] args = {"invalid", "invalid", "invalid", "read"};
        assertThrows(SparkIndexException.class, () -> SolrIndexValidatorMain.main(args));
    }

    @Test
    void testSolrIndexValidatorMainInvalidTaxDb() {
        String[] args = {"2020_04", "uniparc", SPARK_LOCAL_MASTER, "invalid"};
        assertThrows(SparkIndexException.class, () -> SolrIndexValidatorMain.main(args));
    }

    @Test
    void testSolrIndexValidatorMainThrowExceptions() {
        // valid arguments, but it will fail because it will not able to connect to zookeeper.
        String[] args = {"2020_02", "uniparc", SPARK_LOCAL_MASTER, "read"};
        assertThrows(SparkIndexException.class, () -> SolrIndexValidatorMain.main(args));
    }
}
