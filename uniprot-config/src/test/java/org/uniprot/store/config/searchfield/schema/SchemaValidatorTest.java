package org.uniprot.store.config.searchfield.schema;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.schema.SchemaValidator;
import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.TestSearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;

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
        String schemaFile = AbstractSearchFieldConfig.SCHEMA_FILE;
        return Stream.of(
                Arguments.of(SearchFieldConfigFactory.CROSSREF_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.DISEASE_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.GENECENTRIC_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.KEYWORD_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.LITERATURE_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.PROTEOME_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.SUBCELLLOCATION_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.SUGGEST_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.TAXONOMY_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.UNIPARC_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.UNIPROTKB_CONFIG_FILE, schemaFile),
                Arguments.of(SearchFieldConfigFactory.UNIREF_CONFIG_FILE, schemaFile),
                Arguments.of(
                        TestSearchFieldConfig.TEST_SEARCH_FIELDS_CONFIG,
                        TestSearchFieldConfig.TEST_SCHEMA_CONFIG));
    }
}
