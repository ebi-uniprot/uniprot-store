package org.uniprot.store.config.schema;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.returnfield.config.AbstractReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;

/**
 * @author lgonzales
 * @since 2020-03-18
 */
class ReturnFieldSchemaValidatorIT {

    @DisplayName("Test the main config file against the schema")
    @ParameterizedTest
    @MethodSource("provideConfigFiles")
    void testJsonConfig(String configFile, String schemaFile) {
        Assertions.assertDoesNotThrow(
                () -> SchemaValidator.validate(schemaFile, configFile),
                "Validation failed for config file '" + configFile + "'");
    }

    private static Stream<Arguments> provideConfigFiles() {
        String schemaFile = AbstractReturnFieldConfig.SCHEMA_FILE;
        return Stream.of(
                Arguments.of(ReturnFieldConfigFactory.CROSS_REF_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.DISEASE_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.GENE_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.KEYWORD_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.LITERATURE_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.PROTEOME_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.SUBCELL_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.SUGGEST_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.TAXONOMY_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.UNIPARC_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.UNIPROTKB_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.UNIREF_CONFIG_FILE, schemaFile),
                Arguments.of(ReturnFieldConfigFactory.UNIPARC_CROSSREF_CONFIG_FILE, schemaFile));
    }
}
