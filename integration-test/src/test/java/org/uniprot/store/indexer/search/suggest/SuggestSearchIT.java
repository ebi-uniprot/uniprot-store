package org.uniprot.store.indexer.search.suggest;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.contains;
import static org.uniprot.store.search.field.SuggestField.Importance.high;
import static org.uniprot.store.search.field.SuggestField.Importance.medium;

import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.domain2.UniProtSearchFields;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.SuggestField;

class SuggestSearchIT {
    @RegisterExtension static SuggestSearchEngine searchEngine = new SuggestSearchEngine();
    private static final String REQUEST_HANDLER = "/search";

    @AfterEach
    void cleanup() {
        QueryResponse queryResponse = getResponse("*:*");

        queryResponse
                .getResults()
                .forEach(
                        doc ->
                                searchEngine.removeEntry(
                                        doc.getFieldValue(SuggestField.Stored.id.name())
                                                .toString()));
    }

    @Test
    void exactMatch() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary("anotherDictionary")
                        .value(value)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, id));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, id, value, altValue);
    }

    @Test
    void leftPrefixMatchWillHit() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary("anotherDictionary")
                        .value(value)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, id.substring(0, id.length() - 1)));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, id, value, altValue);
    }

    @Test
    void leftPrefixAltValueDMelanogasterMatchWillHit() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("D. melanogaster");
        String dict = "randomDictionary";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary("anotherDictionary")
                        .value(value)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, "mel"));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, id, value, altValue);
    }

    @Test
    void leadingZerosAreIgnored() {
        String nonZeroIdPart = "1234";
        String id = "00000" + nonZeroIdPart;
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id("234")
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, nonZeroIdPart));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, id, value, altValue);
    }

    @Test
    void exactMatchComesFirst() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String idLonger = id + "567";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(idLonger)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, id));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(2));
        checkResultsContains(results, 0, id, value, altValue);
        checkResultsContains(results, 1, idLonger, value, altValue);
    }

    @Test
    void exactMatchOfSecondWord() {
        String id = "1234";
        String value = "one two three four";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String idLonger = id + "567";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(idLonger)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value("another value")
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, "two"));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, idLonger, value, altValue);
    }

    @Test
    void prefixMatchOfSecondWord() {
        String id = "1234";
        String value = "one twoooo three four";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String idLonger = id + "567";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(idLonger)
                        .dictionary(dict)
                        .value(value)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value("another value")
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, "two"));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, idLonger, value, altValue);
    }

    @Test
    void idHasPrecedenceOverValue() {
        String id = "12345678";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String someId = "some id";
        String someValue = "some value";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(someId)
                        .dictionary(dict)
                        .value(id)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(someValue)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, id));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(2));
        checkResultsContains(results, 0, id, someValue, altValue);
        checkResultsContains(results, 1, someId, id, altValue);
    }

    @Test
    void multiWordIdHasPrecedenceOverValue() {
        String id = "1234 5678";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String someId = "some id";
        String someValue = "some value";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(someId)
                        .dictionary(dict)
                        .value(id)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(someValue)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, id));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(2));
        checkResultsContains(results, 0, id, someValue, altValue);
        checkResultsContains(results, 1, someId, id, altValue);
    }

    @Test
    void exactAltValueHasPrecedenceOverAnother() {
        String dict = "randomDictionary";
        String id = "id";
        String someValue = "someValue";
        String man = "Man";
        List<String> altValues = asList(man, "Human", "another", "yetAnother");

        String otherId = "otherId";
        List<String> otherAltValues = asList("Human", man + " another", "yetAnother");

        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(otherId)
                        .dictionary(dict)
                        .importance(medium.name())
                        .value(someValue)
                        .altValues(otherAltValues)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .importance(high.name())
                        .value(someValue)
                        .altValues(altValues)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, man.toLowerCase()));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(2));
        checkResultsContains(results, 0, id, someValue, altValues);
        checkResultsContains(results, 1, otherId, someValue, otherAltValues);
    }

    @Test
    void findsECTypeId() {
        String id = "1.2.3.1";
        String someValue = "some value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(someValue)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, id));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, id, someValue, altValue);
    }

    @Test
    void findsPrefixECTypeId() {
        String prefixId = "1.2";
        String id = prefixId + "3.1";
        String someValue = "some value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String someId = "some id";
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(id)
                        .dictionary(dict)
                        .value(someValue)
                        .altValues(altValue)
                        .build());
        searchEngine.indexEntry(
                SuggestDocument.builder()
                        .id(someId)
                        .dictionary(dict)
                        .value(someValue)
                        .altValues(altValue)
                        .build());

        QueryResponse queryResponse = getResponse(query(dict, prefixId));

        SolrDocumentList results = queryResponse.getResults();
        assertThat(results, hasSize(1));
        checkResultsContains(results, 0, id, someValue, altValue);
    }

    private QueryResponse getResponse(String query) {
        return searchEngine.getQueryResponse(REQUEST_HANDLER, query);
    }

    private void checkResultsContains(
            SolrDocumentList results,
            int position,
            String id,
            String value,
            List<String> altValues) {
        SolrDocument document = results.get(position);
        checkFieldForDocument(document, SuggestField.Stored.id, id);
        checkFieldForDocument(document, SuggestField.Stored.value, value);
        checkFieldForDocument(document, SuggestField.Stored.altValue, altValues);
    }

    private void checkFieldForDocument(
            SolrDocument document, SuggestField.Stored valueEnum, String value) {
        Collection<String> fieldNames = document.getFieldNames();

        if (value == null) {
            assertThat(fieldNames, not(contains(valueEnum.name())));
        } else {
            assertThat(document.getFieldValue(valueEnum.name()), is(value));
        }
    }

    private void checkFieldForDocument(
            SolrDocument document, SuggestField.Stored valueEnum, List<String> values) {
        Collection<String> fieldNames = document.getFieldNames();

        if (values == null) {
            assertThat(fieldNames, not(contains(valueEnum.name())));
        } else {
            assertThat(document.getFieldValue(valueEnum.name()), is(values));
        }
    }

    private String query(String dict, String content) {
        String query =
                "\""
                        + content
                        + "\""
                        + " +"
                        + QueryBuilder.query(
                                UniProtSearchFields.SUGGEST.getField("dict").getName(), dict);
        System.out.println(query);
        return query;
    }
}
