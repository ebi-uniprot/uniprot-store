package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.domain2.SearchItem;
import org.uniprot.store.search.domain2.UniProtKBSearchItems;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * This class verifies example values against their associated fields, defined in {@code search-fields.json}. These
 * examples help clients (e.g., front-end) formulate the correct format of queries.
 *
 * Created 18/11/2019
 *
 * @author Edd
 */
class VerifyUniProtAdvancedSearchExamplesIT {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @ParameterizedTest(name = "{1}:{2}")
    @MethodSource("provideSearchItems")
    void searchFieldIsKnownToSearchEngine(
            String label, String field, String example) {
        assertThat(label, is(not(isEmptyOrNullString())));
        assertThat(example, is(not(isEmptyOrNullString())));

        assertThat(field, is(not(isEmptyOrNullString())));

        QueryResponse queryResponse =
                searchEngine.getQueryResponse("select", field + ":" + example);
        assertThat(queryResponse, is(notNullValue()));
    }

    private static Stream<Arguments> provideSearchItems() {
        List<SearchItem> items = new ArrayList<>(UniProtKBSearchItems.INSTANCE.getSearchItems());
        return extractAllItems(items).stream()
                .map(
                        searchField ->
                                Arguments.of(
                                        searchField.getLabel(),
                                        searchField.getField() != null
                                                ? searchField.getField()
                                                : searchField.getRangeField(),
                                        searchField.getExample()));
    }

    private static List<SearchItem> extractAllItems(List<SearchItem> items) {
        List<SearchItem> currentItems = new ArrayList<>();
        extractAllItems(items, currentItems);
        return currentItems;
    }

    private static void extractAllItems(List<SearchItem> items, List<SearchItem> currentItems) {
        for (SearchItem item : items) {
            if (item.getItemType().equals("single")) {
                currentItems.add(item);
            }
            if (Utils.notNullOrEmpty(item.getItems())) {
                extractAllItems(item.getItems(), currentItems);
            }
        }
    }
}
