package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.domain2.SearchItem;
import org.uniprot.store.search.domain2.UniProtKBSearchFields;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * Created 18/11/2019
 *
 * @author Edd
 */
class VerifyUniProtAdvancedSearchExamplesIT {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    private static Stream<Arguments> provideSearchItems() {
        List<SearchItem> items =
                new ArrayList<>(UniProtKBSearchFields.INSTANCE.getSearchItems());
        return extractAllItems(items).stream().map(Arguments::of);
    }

    private static List<SearchItem> extractAllItems(List<SearchItem> items) {
        List<SearchItem> currentItems = new ArrayList<>();
        extractAllItems(items, currentItems);
        return currentItems;
    }

    private static void extractAllItems(
            List<SearchItem> items, List<SearchItem> currentItems) {
        for (SearchItem item : items) {
            if (item.getItemType().equals("single")) {
                currentItems.add(item);
            }
            if (Utils.notNullOrEmpty(item.getItems())) {
                extractAllItems(item.getItems(), currentItems);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideSearchItems")
    void searchFieldIsKnownToSearchEngine(SearchItem searchItem) {
        assertThat(searchItem.getLabel(), is(not(isEmptyOrNullString())));
        String example = searchItem.getExample();
        String field = searchItem.getField();
        assertThat(example, is(not(isEmptyOrNullString())));

        if (field == null) {
            String rangeField = searchItem.getRangeField();
            assertThat(rangeField, is(not(isEmptyOrNullString())));

            field = rangeField;
        }
        
        assertThat(field, is(not(isEmptyOrNullString())));

        // found need to use date queries like: (created:[2010-10-08T23:59:59Z TO 2019-11-22T23:59:59Z]), not simple dates
        QueryResponse queryResponse =
                searchEngine.getQueryResponse("select", field + ":" + example);
        assertThat(queryResponse, is(notNullValue()));
    }
}
