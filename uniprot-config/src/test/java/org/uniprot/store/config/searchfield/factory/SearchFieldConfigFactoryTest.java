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

    private static Stream<Arguments> provideTypeAndItemCount() {
        return Stream.of(
                Arguments.of(UniProtDataType.CROSSREF, 7),
                Arguments.of(UniProtDataType.DISEASE, 4),
                Arguments.of(UniProtDataType.GENECENTRIC, 8),
                Arguments.of(UniProtDataType.KEYWORD, 7),
                Arguments.of(UniProtDataType.LITERATURE, 11),
                Arguments.of(UniProtDataType.PROTEOME, 18),
                Arguments.of(UniProtDataType.SUBCELLLOCATION, 6),
                Arguments.of(UniProtDataType.SUGGEST, 3),
                Arguments.of(UniProtDataType.TAXONOMY, 16),
                Arguments.of(UniProtDataType.UNIPARC, 15),
                Arguments.of(UniProtDataType.UNIPROTKB, 432),
                Arguments.of(UniProtDataType.UNIREF, 17));
    }
}