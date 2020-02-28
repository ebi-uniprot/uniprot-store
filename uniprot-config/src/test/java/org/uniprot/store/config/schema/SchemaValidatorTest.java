package org.uniprot.store.config.schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.uniprotkb.TestFieldConfiguration;
import org.uniprot.store.config.uniprotkb.UniProtKBSearchFieldConfiguration;

public class SchemaValidatorTest {

    @DisplayName("Test the main config file against the schema")
    @Test
    void testJsonConfig() {
        Assertions.assertDoesNotThrow(
                () ->
                        SchemaValidator.validate(
                                UniProtKBSearchFieldConfiguration.SCHEMA_FILE,
                                UniProtKBSearchFieldConfiguration.CONFIG_FILE),
                "Validation failed");
    }

    @DisplayName("Test the test config file against the test schema")
    @Test
    void testTestJsonConfig() {
        Assertions.assertDoesNotThrow(
                () ->
                        SchemaValidator.validate(
                                TestFieldConfiguration.TEST_SCHEMA_CONFIG,
                                TestFieldConfiguration.TEST_SEARCH_FIELDS_CONFIG),
                "Test config file validation failed");
    }
}
