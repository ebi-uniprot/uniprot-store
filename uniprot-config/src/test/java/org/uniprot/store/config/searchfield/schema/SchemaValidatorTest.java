package org.uniprot.store.config.searchfield.schema;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.common.TestSearchFieldConfig;
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
                Arguments.of(CrossRefSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(DiseaseSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(GeneCentricSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(KeywordSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(LiteratureSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(ProteomeSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(SubcellLocationSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(SuggestSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(TaxonomySearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(UniParcSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(UniProtKBSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(UniRefSearchFieldConfig.CONFIG_FILE, schemaFile),
                Arguments.of(
                        TestSearchFieldConfig.TEST_SEARCH_FIELDS_CONFIG,
                        TestSearchFieldConfig.TEST_SCHEMA_CONFIG));
    }
}
