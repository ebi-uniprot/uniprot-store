package org.uniprot.store.config.searchfield.factory;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.impl.*;

public class SearchFieldConfigFactoryTest {

    @ParameterizedTest
    @MethodSource("provideTypeAndClass")
    void testObjectionCreation(UniProtDataType dataType, Class<? extends SearchFieldConfig> clazz) {
        Assertions.assertEquals(
                clazz, SearchFieldConfigFactory.getSearchFieldConfig(dataType).getClass());
    }

    @Test
    void testWithNullDataType() {
        Assertions.assertThrows(
                NullPointerException.class,
                () -> SearchFieldConfigFactory.getSearchFieldConfig(null));
    }

    private static Stream<Arguments> provideTypeAndClass() {
        return Stream.of(
                Arguments.of(UniProtDataType.crossref, CrossRefSearchFieldConfig.class),
                Arguments.of(UniProtDataType.disease, DiseaseSearchFieldConfig.class),
                Arguments.of(UniProtDataType.genecentric, GeneCentricSearchFieldConfig.class),
                Arguments.of(UniProtDataType.keyword, KeywordSearchFieldConfig.class),
                Arguments.of(UniProtDataType.literature, LiteratureSearchFieldConfig.class),
                Arguments.of(UniProtDataType.proteome, ProteomeSearchFieldConfig.class),
                Arguments.of(
                        UniProtDataType.subcelllocation, SubcellLocationSearchFieldConfig.class),
                Arguments.of(UniProtDataType.suggest, SuggestSearchFieldConfig.class),
                Arguments.of(UniProtDataType.taxonomy, TaxonomySearchFieldConfig.class),
                Arguments.of(UniProtDataType.uniparc, UniParcSearchFieldConfig.class),
                Arguments.of(UniProtDataType.uniprotkb, UniProtKBSearchFieldConfig.class),
                Arguments.of(UniProtDataType.uniref, UniRefSearchFieldConfig.class));
    }
}
