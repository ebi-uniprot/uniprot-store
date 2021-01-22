package org.uniprot.store.config.returnfield.config.impl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.parser.tsv.unirule.UniRuleEntryValueMapper;
import org.uniprot.core.unirule.ConditionSet;
import org.uniprot.core.unirule.Rule;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.unirule.impl.*;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Created 30/11/20
 *
 * @author sahmad
 */
class UniRuleReturnFieldConfigImplIT {

    private static UniRuleEntry entry;
    private static ReturnFieldConfig returnFieldConfig;
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void setUp() {
        returnFieldConfig = ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIRULE);
        searchFieldConfig = SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.UNIRULE);
        ConditionBuilder conditionBuilder = new ConditionBuilder("taxon");
        conditionBuilder.conditionValuesAdd(
                new ConditionValueBuilder("Archaea").cvId("2157").build());
        conditionBuilder.conditionValuesAdd(
                new ConditionValueBuilder("Eukaryota").cvId("2759").build());
        conditionBuilder.conditionValuesAdd(
                new ConditionValueBuilder("Bacteria").cvId("2").build());
        ConditionSet conditionSet = new ConditionSetBuilder(conditionBuilder.build()).build();

        Rule mainRule =
                new RuleBuilder(conditionSet)
                        .annotationsAdd(AnnotationBuilderTest.createObject())
                        .build(); // update main rule
        entry = UniRuleEntryBuilderTest.createObject(1, true);
        UniRuleEntryBuilder builder = UniRuleEntryBuilder.from(entry);
        builder.mainRule(mainRule);
        entry = builder.build();
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
        UniRuleEntryValueMapper entityValueMapper = new UniRuleEntryValueMapper();
        Map<String, String> mappedField =
                entityValueMapper.mapEntity(entry, Collections.singletonList(returnFieldName));
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
