package org.uniprot.store.spark.indexer.main.verifiers;

import org.junit.jupiter.api.Test;

import java.net.BindException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

class ValidateHPSDocumentsMainTest {
    @Test
    void errorWithInvalidArguments() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ValidateHPSDocumentsMain.main(new String[3]));
    }

    @Test
    void testRunValidation() {
        String[] args = {"uniprot", SPARK_LOCAL_MASTER};
        assertThrows(
                BindException.class,() -> ValidateHPSDocumentsMain.main(args));
    }
}
