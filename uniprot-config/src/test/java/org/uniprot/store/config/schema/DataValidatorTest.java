package org.uniprot.store.config.schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.uniprotkb.TestFieldConfiguration;

public class DataValidatorTest {
    @DisplayName("Test data validator")
    @Test
    void testDataValidator() {
        Assertions.assertDoesNotThrow(() -> TestFieldConfiguration.getInstance());
    }
}
