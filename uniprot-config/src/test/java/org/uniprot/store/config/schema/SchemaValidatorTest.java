package org.uniprot.store.config.schema;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.common.SearchFieldConfiguration;
import org.uniprot.store.config.common.TestFieldConfiguration;
import org.uniprot.store.config.crossref.CrossRefSearchFieldConfiguration;
import org.uniprot.store.config.uniprotkb.UniProtKBSearchFieldConfiguration;

public class SchemaValidatorTest {

    @DisplayName("Test the main config file against the schema")
    @ParameterizedTest
    @MethodSource("provideConfigFiles")
    void testJsonConfig(String configFile, String schemaFile) {
        Assertions.assertDoesNotThrow(
                () -> SchemaValidator.validate(schemaFile, configFile),
                "Validation failed for config file '" + configFile + "'");
    }

    private static Stream<Arguments> provideConfigFiles() {
        String schemaFile = SearchFieldConfiguration.SCHEMA_FILE;
        return Stream.of(
                Arguments.of(UniProtKBSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(CrossRefSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(
                        TestFieldConfiguration.TEST_SEARCH_FIELDS_CONFIG,
                        TestFieldConfiguration.TEST_SCHEMA_CONFIG));
    }
}
