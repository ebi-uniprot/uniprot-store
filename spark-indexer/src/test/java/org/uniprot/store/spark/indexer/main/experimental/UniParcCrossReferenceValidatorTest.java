package org.uniprot.store.spark.indexer.main.experimental;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.CommonVariables;

import static org.junit.jupiter.api.Assertions.*;

class UniParcCrossReferenceValidatorTest {

    @Test
    void testUniParcCrossReferenceValidatorInvalidArgumentIndex() {
        assertThrows(IllegalArgumentException.class, () -> UniParcCrossReferenceValidator.main(new String[0]));
    }

    @Test
    void testUniParcCrossReferenceValidatorWillFailToConnectToVoldemort() {
        String[] args = {"2020_02", CommonVariables.SPARK_LOCAL_MASTER};
        assertThrows(IndexDataStoreException.class, () -> UniParcCrossReferenceValidator.main(args));
    }
}