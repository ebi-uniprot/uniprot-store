package org.uniprot.store.spark.indexer.main.verifiers;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.net.BindException;

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
        assertThrows(BindException.class, () -> ValidateHPSDocumentsMain.main(args));
    }
}
