package org.uniprot.store.schema;

import static org.uniprot.store.config.UniProtSearchFieldConfiguration.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class SchemaValidatorTest {
    private static final String TEST_SEARCH_FIELDS_CONFIG =
            "src/test/resources/test-uniprot-fields.json";
    private static final String TEST_SCHEMA_CONFIG = "src/test/resources/test-schema.json";

    @DisplayName("Test the main config file against the schema")
    @Test
    void testJsonConfig() {
        Assertions.assertDoesNotThrow(
                () -> SchemaValidator.validate(SCHEMA_FILE, CONFIG_FILE), "Validation failed");
    }

    @DisplayName("Test the test config file against the test schema")
    @Test
    void testTestJsonConfig() {
        Assertions.assertDoesNotThrow(
                () -> SchemaValidator.validate(TEST_SCHEMA_CONFIG, TEST_SEARCH_FIELDS_CONFIG),
                "Test config file validation failed");
    }
}
