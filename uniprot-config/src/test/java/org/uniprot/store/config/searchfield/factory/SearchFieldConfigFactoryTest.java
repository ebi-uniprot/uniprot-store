package org.uniprot.store.config.searchfield.factory;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.impl.SearchFieldConfigImpl;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

class SearchFieldConfigFactoryTest {

    @ParameterizedTest(name = "[{index}] ItemCount({0}) == {1} ?")
    @MethodSource("provideTypeAndItemCount")
    void testObjectionCreation(UniProtDataType dataType, Integer itemCount) {
        SearchFieldConfig searchFieldConfig =
                SearchFieldConfigFactory.getSearchFieldConfig(dataType);
        Assertions.assertEquals(SearchFieldConfigImpl.class, searchFieldConfig.getClass());
        Assertions.assertNotNull(searchFieldConfig.getAllFieldItems());
        Assertions.assertEquals(itemCount, searchFieldConfig.getAllFieldItems().size());
    }

    @Test
    void testWithNullDataType() {
        Assertions.assertThrows(
                NullPointerException.class,
                () -> SearchFieldConfigFactory.getSearchFieldConfig(null));
    }

    @ParameterizedTest(name = "[{0}] ValuesCount({1}) == {2} ?")
    @MethodSource("provideValuesFieldAndCount")
    void testValuesForField(UniProtDataType dataType, String fieldName, Integer valuesCount) {
        SearchFieldConfig searchFieldConfig =
                SearchFieldConfigFactory.getSearchFieldConfig(dataType);
        Assertions.assertEquals(SearchFieldConfigImpl.class, searchFieldConfig.getClass());
        Assertions.assertNotNull(searchFieldConfig.getAllFieldItems());
        SearchFieldItem fieldItem = searchFieldConfig.getSearchFieldItemByName(fieldName);
        Assertions.assertNotNull(fieldItem.getValues());
        Assertions.assertEquals(valuesCount, fieldItem.getValues().size());
    }

    private static Stream<Arguments> provideTypeAndItemCount() {
        int uniProtKBDBTypesCount = UniProtDatabaseTypes.INSTANCE.getAllDbTypes().size();
        return Stream.of(
                Arguments.of(UniProtDataType.CROSSREF, 6),
                Arguments.of(UniProtDataType.DISEASE, 3),
                Arguments.of(UniProtDataType.GENECENTRIC, 7),
                Arguments.of(UniProtDataType.KEYWORD, 9),
                Arguments.of(UniProtDataType.LITERATURE, 11),
                Arguments.of(UniProtDataType.PROTEOME, 21),
                Arguments.of(UniProtDataType.PUBLICATION, 7),
                Arguments.of(UniProtDataType.SUBCELLLOCATION, 7),
                Arguments.of(UniProtDataType.SUGGEST, 3),
                Arguments.of(UniProtDataType.TAXONOMY, 15),
                Arguments.of(UniProtDataType.UNIPARC, 18),
                Arguments.of(UniProtDataType.UNIPROTKB, 444 + uniProtKBDBTypesCount),
                Arguments.of(UniProtDataType.UNIREF, 17),
                Arguments.of(UniProtDataType.UNIRULE, 30),
                Arguments.of(UniProtDataType.HELP, 8),
                Arguments.of(UniProtDataType.ARBA, 23));
    }

    private static Stream<Arguments> provideValuesFieldAndCount() {
        return Stream.of(
                Arguments.of(UniProtDataType.PROTEOME, "cpd", 6),
                Arguments.of(UniProtDataType.PROTEOME, "proteome_type", 4));
    }
}
