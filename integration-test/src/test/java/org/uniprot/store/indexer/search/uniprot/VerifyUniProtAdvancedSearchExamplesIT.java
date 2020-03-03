package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;

import java.util.List;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

/**
 * This class verifies example values against their associated fields, defined in {@code
 * uniprotkb-search-fields.json}. These examples help clients (e.g., front-end) formulate the
 * correct format of queries.
 *
 * <p>Created 18/11/2019
 *
 * @author Edd
 */
class VerifyUniProtAdvancedSearchExamplesIT {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @ParameterizedTest(name = "{0}:{1}")
    @MethodSource("provideSearchItems")
    void searchFieldIsKnownToSearchEngine(String field, String example) {
        assertThat("example is empty for field " + field, example, is(not(isEmptyOrNullString())));

        assertThat(field, is(not(isEmptyOrNullString())));

        QueryResponse queryResponse =
                searchEngine.getQueryResponse("select", field + ":" + example);
        assertThat(queryResponse, is(notNullValue()));
    }

    private static Stream<Arguments> provideSearchItems() {
        List<SearchFieldItem> items = searchEngine.getSearchFieldConfig().getSearchFieldItems();
        return items.stream()
                .filter(fieldItem -> SearchFieldItemType.SINGLE.equals(fieldItem.getItemType()))
                .map(fi -> Arguments.of(fi.getFieldName(), fi.getExample()));
    }
}
