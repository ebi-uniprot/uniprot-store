package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.search.domain2.SearchItem;
import org.uniprot.store.search.domain2.UniProtKBSearchFields;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created 18/11/2019
 *
 * @author Edd
 */
public class VerifyUniProtAdvancedSearchExamplesIT {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    private static Stream<Arguments> provideSearchItems() {
        return UniProtKBSearchFields.INSTANCE.getSearchItems().stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("provideSearchItems")
    void searchFieldIsKnownToSearchEngine(SearchItem searchItem) {
        assertThat(searchItem.getLabel(), is(not(isEmptyOrNullString())));
        assertThat(searchItem.getExample(), is(not(isEmptyOrNullString())));

//        searchItem.
        QueryResponse queryResponse = searchEngine.getQueryResponse("select", searchItem + ":*");
        assertThat(queryResponse, is(notNullValue()));
    }

    @Test
    void unknownFieldCausesException() {
        assertThrows(SolrException.class, () -> searchEngine.getQueryResponse("select", "asdf:*"));
    }
}
