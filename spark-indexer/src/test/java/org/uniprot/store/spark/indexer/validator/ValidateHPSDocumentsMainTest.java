package org.uniprot.store.spark.indexer.validator;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.junit.jupiter.api.Test;

class ValidateHPSDocumentsMainTest {
    @Test
    void errorWithInvalidArguments() {
        assertThrows(
                IllegalArgumentException.class, () -> ValidateHPSDocumentsMain.main(new String[3]));
    }

    @Test
    void testRunValidation() {
        String[] args = {"uniprot", SPARK_LOCAL_MASTER};
        assertThrows(Exception.class, () -> ValidateHPSDocumentsMain.main(args));
    }
}
