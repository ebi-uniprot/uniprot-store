package org.uniprot.store.search.domain2;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtXDbTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.search.domain2.UniProtKBSearchFields.INSTANCE;
import static org.uniprot.store.search.domain2.UniProtKBSearchFields.XREF_COUNT_PREFIX;

/**
 * Created 19/11/2019
 *
 * @author Edd
 */
class UniProtKBSearchFieldsTest {
    @Test
    void canLoadAllFields() {
        List<String> fieldNames =
                getFields(searchField -> !searchField.getName().startsWith(XREF_COUNT_PREFIX))
                        .stream()
                        .map(SearchField::getName)
                        .collect(Collectors.toList());
        assertThat(
                fieldNames,
                containsInAnyOrder(
                        "annotation_score",
                        "mnemonic_default",
                        "accession",
                        "ec",
                        "cc_cofactor_chebi",
                        "cc_cofactor_note",
                        "ccev_cofactor_chebi",
                        "ccev_cofactor_note",
                        "pretend_cofactor_range_field"));
    }

    @Test
    void checkSortsAreCorrect() {
        assertThat(INSTANCE.getSorts(), containsInAnyOrder("accession_id", "annotation_score"));
    }

    @Test
    void checkRangeFieldsAreCorrect() {
        assertThat(
                INSTANCE.getRangeFields().stream()
                        .map(SearchField::getName)
                        .collect(Collectors.toList()),
                containsInAnyOrder("pretend_cofactor_range_field"));
    }

    @Test
    void checkGeneralFieldsAreCorrect() {
        List<String> generalFields =
                INSTANCE.getGeneralFields().stream()
                        .map(SearchField::getName)
                        .filter(searchField -> !searchField.startsWith(XREF_COUNT_PREFIX))
                        .collect(Collectors.toList());
        assertThat(
                generalFields,
                containsInAnyOrder(
                        "annotation_score",
                        "mnemonic_default",
                        "accession",
                        "ec",
                        "cc_cofactor_chebi",
                        "cc_cofactor_note",
                        "ccev_cofactor_chebi",
                        "ccev_cofactor_note"));
    }

    @Test
    void checkXrefCountFieldsIncludeAllDatabaseTypes() {
        assertThat(
                getFields(searchField -> searchField.getName().startsWith(XREF_COUNT_PREFIX)),
                hasSize(UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().size()));
    }

    @Test
    void searchItemsAreCorrect() {
        Map<String, SearchItem> itemMap =
                searchItemsToMap(UniProtKBSearchFields.INSTANCE.getSearchItems());
        assertThat(
                itemMap.keySet(),
                containsInAnyOrder("ACCESSION", "FUNCTION", "COFACTORS", "CHEBI", "NOTE", "EC"));

        SearchItem accessionItem = itemMap.get("ACCESSION");
        assertThat(accessionItem.getField(), is("accession"));
        assertThat(accessionItem.getSortField(), is("accession_id"));
        assertThat(accessionItem.getItemType(), is("single"));
        assertThat(accessionItem.getDescription(), is("Accession description"));
        assertThat(accessionItem.getExample(), is("P12345"));

        SearchItem chebiItem = itemMap.get("CHEBI");
        assertThat(chebiItem.getField(), is("cc_cofactor_chebi"));
        assertThat(chebiItem.getEvidenceField(), is("ccev_cofactor_chebi"));
        assertThat(chebiItem.getDataType(), is("string"));
        assertThat(chebiItem.getAutoComplete(), is("/uniprot/api/suggester?dict=chebi&query=?"));
        assertThat(chebiItem.getItemType(), is("single"));
        assertThat(chebiItem.getDescription(), is("Search by cofactor chebi"));
        assertThat(chebiItem.getExample(), is("29105"));
        assertThat(chebiItem.getIdField(), is("cc_cofactor_chebi"));

        checkIsGroup("FUNCTION", itemMap);
        checkIsGroup("COFACTORS", itemMap);
    }

    private void checkIsGroup(String groupName, Map<String, SearchItem> itemMap) {
        SearchItem functionItem = itemMap.get(groupName);
        assertThat(functionItem.getItemType(), is("group"));
        assertThat(functionItem.getField(), is(nullValue()));
        assertThat(functionItem.getSortField(), is(nullValue()));
        assertThat(functionItem.getDescription(), is(nullValue()));
        assertThat(functionItem.getExample(), is(nullValue()));
    }

    private Map<String, SearchItem> searchItemsToMap(List<SearchItem> items) {
        Map<String, SearchItem> itemMap = new HashMap<>();
        searchItemsToMap(items, itemMap);
        return itemMap;
    }

    private void searchItemsToMap(List<SearchItem> items, Map<String, SearchItem> itemMap) {
        for (SearchItem item : items) {
            itemMap.put(item.getLabel(), item);
            if (item.getItems() != null && !item.getItems().isEmpty()) {
                searchItemsToMap(item.getItems(), itemMap);
            }
        }
    }

    private Set<SearchField> getFields(Predicate<SearchField> predicate) {
        return UniProtKBSearchFields.INSTANCE.getSearchFields().stream()
                .filter(predicate)
                .peek(System.out::println)
                .collect(Collectors.toSet());
    }
}
