package org.uniprot.store.config.searchfield.schema;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.common.TestFieldConfiguration;
import org.uniprot.store.config.searchfield.impl.*;

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
        String schemaFile = SearchFieldConfig.SCHEMA_FILE;
        return Stream.of(
                Arguments.of(CrossRefSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(DiseaseSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(GeneCentricSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(KeywordSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(LiteratureSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(ProteomeSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(SubcellLocationSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(SuggestSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(TaxonomySearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(UniParcSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(UniProtKBSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(UniRefSearchFieldConfiguration.CONFIG_FILE, schemaFile),
                Arguments.of(
                        TestFieldConfiguration.TEST_SEARCH_FIELDS_CONFIG,
                        TestFieldConfiguration.TEST_SCHEMA_CONFIG));
    }
}
