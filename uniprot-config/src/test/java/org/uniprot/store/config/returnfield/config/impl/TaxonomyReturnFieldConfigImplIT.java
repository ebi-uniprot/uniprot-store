package org.uniprot.store.config.returnfield.config.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.json.parser.taxonomy.TaxonomyEntryTest;
import org.uniprot.core.parser.tsv.taxonomy.TaxonomyEntryValueMapper;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

/**
 * @author lgonzales
 * @since 2020-03-26
 */
class TaxonomyReturnFieldConfigImplIT {
    private static TaxonomyEntry entry;
    private static ReturnFieldConfig returnFieldConfig;
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void setUp() {
        returnFieldConfig = ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.TAXONOMY);
        searchFieldConfig = SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.TAXONOMY);
        entry = TaxonomyEntryTest.getCompleteTaxonomyEntry();
    }

    @ParameterizedTest(
            name = "Sort field [{0}] configured in return field exists in search fields?")
    @MethodSource("provideReturnSortFields")
    void validReturnFieldSortFieldDefined(String returnFieldsSortField) {
        assertThat(searchFieldConfig.correspondingSortFieldExists(returnFieldsSortField), is(true));
    }

    @ParameterizedTest(name = "Sort field [{0}] configured in search exists in return fields?")
    @MethodSource("provideSearchSortFields")
    void validSearchFieldSortFieldDefined(String searchFieldsSortField) {
        boolean found =
                returnFieldConfig.getReturnFields().stream()
                        .map(ReturnField::getSortField)
                        .filter(Objects::nonNull)
                        .map(searchFieldConfig::getCorrespondingSortField)
                        .map(SearchFieldItem::getFieldName)
                        .anyMatch(
                                sortFieldName ->
                                        sortFieldName.equalsIgnoreCase(searchFieldsSortField));
        assertTrue(found);
    }

    @ParameterizedTest(name = "Return TSV column [{0}] for return field exists?")
    @MethodSource("provideReturnFieldNames")
    void validReturnFieldWithMappedEntryDefined(String returnFieldName) {
        TaxonomyEntryValueMapper entityValueMapper = new TaxonomyEntryValueMapper();
        Map<String, String> mappedField =
                entityValueMapper.mapEntity(entry, Collections.singletonList(returnFieldName));
        System.out.println(returnFieldName + " : " + mappedField.get(returnFieldName));
        assertNotNull(mappedField.get(returnFieldName));
        assertFalse(mappedField.get(returnFieldName).isEmpty());
    }

    private static Stream<Arguments> provideSearchSortFields() {
        return searchFieldConfig.getSortFieldItems().stream()
                .map(SearchFieldItem::getFieldName)
                .map(Arguments::of);
    }

    private static Stream<Arguments> provideReturnSortFields() {
        return returnFieldConfig.getReturnFields().stream()
                .filter(field -> Utils.notNullNotEmpty(field.getSortField()))
                .map(ReturnField::getSortField)
                .map(Arguments::of);
    }

    private static Stream<Arguments> provideReturnFieldNames() {
        return returnFieldConfig.getReturnFields().stream()
                .map(ReturnField::getName)
                .map(Arguments::of);
    }
}
