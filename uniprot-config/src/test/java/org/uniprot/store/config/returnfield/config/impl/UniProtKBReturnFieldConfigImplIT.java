package org.uniprot.store.config.returnfield.config.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.config.returnfield.config.impl.UniProtKBReturnFieldConfigImpl.DBNAMEPATH;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.json.parser.uniprot.UniProtKBEntryIT;
import org.uniprot.core.parser.tsv.uniprot.UniProtKBEntryValueMapper;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.factory.ReturnFieldConfigFactory;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

/**
 * Created 17/03/20
 *
 * @author Edd
 */
class UniProtKBReturnFieldConfigImplIT {

    private static UniProtKBEntry entry;
    private static ReturnFieldConfig returnFieldConfig;
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void setUp() {
        returnFieldConfig =
                ReturnFieldConfigFactory.getReturnFieldConfig(UniProtDataType.UNIPROTKB);
        searchFieldConfig =
                SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.UNIPROTKB);
        entry = UniProtKBEntryIT.getCompleteColumnsUniProtEntry();
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
        UniProtKBEntryValueMapper entityValueMapper = new UniProtKBEntryValueMapper();
        Map<String, String> mappedField =
                entityValueMapper.mapEntity(entry, Collections.singletonList(returnFieldName));
        // fields not supported by TSV
        if (!returnFieldName.equals("tools") && !returnFieldName.equals("inactiveReason")) {
            assertNotNull(mappedField.get(returnFieldName));
            assertFalse(mappedField.get(returnFieldName).isEmpty());
        }
    }

    @Test
    void testDBSNPPath() {
        Optional<ReturnField> dbSNP =
                returnFieldConfig.getReturnFields().stream()
                        .filter(rf -> "xref_dbsnp".equals(rf.getName()))
                        .findAny();
        assertTrue(dbSNP.isPresent());
        assertEquals(1, dbSNP.get().getPaths().size());
        assertEquals(DBNAMEPATH.get("dbsnp"), dbSNP.get().getPaths().get(0));
    }

    @Test
    void testInternalReturnFields() {
        List<ReturnField> internal =
                returnFieldConfig.getReturnFields().stream()
                        .filter(rf -> Objects.isNull(rf.getParentId()))
                        .collect(Collectors.toList());
        assertNotNull(internal);
        assertEquals(1, internal.size());
        ReturnField inactiveField = internal.get(0);
        assertEquals("inactiveReason", inactiveField.getName());
        assertTrue(inactiveField.getIsRequiredForJson());
    }

    @Test
    void testXRefPathMinusDBSNP() {
        returnFieldConfig.getReturnFields().stream()
                .filter(rf -> rf.getName().startsWith("xref"))
                .filter(rf -> !"xref_proteomes".equals(rf.getName()))
                .filter(rf -> !"xref_dbsnp".equals(rf.getName()))
                .forEach(
                        rf ->
                                assertEquals(
                                        "uniProtKBCrossReferences[?(@.database=='"
                                                + rf.getLabel()
                                                + "')]",
                                        rf.getPaths().get(0),
                                        "Path doesn't match for xref " + rf.getLabel()));
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
