package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.search.domain2.SearchField;
import org.uniprot.store.search.domain2.UniProtKBSearchFields;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created 18/11/2019
 *
 * @author Edd
 */
class ValidateAllFieldsExistIT {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    private static Stream<Arguments> provideSearchFields() {
        return Stream.concat(
                        UniProtKBSearchFields.INSTANCE.getSorts().stream(),
                        UniProtKBSearchFields.INSTANCE.getSearchFields().stream()
                                .map(SearchField::getTerm))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("provideSearchFields")
    void searchFieldIsKnownToSearchEngine(String searchField) {
        QueryResponse queryResponse =
                searchEngine.getQueryResponse("select", searchField + ":*");
        assertThat(queryResponse, is(notNullValue()));
    }

    @Test
    void unknownFieldCausesException() {
        assertThrows(
            SolrException.class,
            () -> searchEngine.getQueryResponse("select", "asdf:*"));
    }
}
