package org.uniprot.store.search.domain2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Created 20/11/19
 *
 * @author Edd
 */
class SearchFieldsLoaderTest {
    private static final String FILENAME = "uniprot/search-fields.json";
    private static SearchFieldsLoader fieldsLoader;

    @BeforeAll
    static void setUp() {
        fieldsLoader = new SearchFieldsLoader(FILENAME);
    }

    @Test
    void canLoadAllFields() {
        List<String> allFieldNames =
                fieldsLoader.getSearchFields().stream()
                        .map(SearchField::getName)
                        .collect(Collectors.toList());

        assertThat(
                allFieldNames,
                containsInAnyOrder(
                        "annotation_score",
                        "mnemonic_default",
                        "accession",
                        "ec",
                        "cc_cofactor_chebi",
                        "cc_cofactor_note",
                        "ccev_cofactor_chebi",
                        "ccev_cofactor_note",
                        "pretend_range_field"));
    }

    @Test
    void checkSortFields() {
        assertThat(fieldsLoader.getSorts(), containsInAnyOrder("accession_id", "annotation_score"));
    }

    @Test
    void checkSearchItems() {
        Map<String, SearchItem> itemMap = searchItemsToMap(fieldsLoader.getSearchItems());
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
        assertThat(chebiItem.getRangeField(), is("pretend_range_field"));
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
        return fieldsLoader.getSearchFields().stream()
                .filter(predicate)
                .peek(System.out::println)
                .collect(Collectors.toSet());
    }
}
