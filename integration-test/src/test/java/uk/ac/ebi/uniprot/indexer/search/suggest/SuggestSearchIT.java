package uk.ac.ebi.uniprot.indexer.search.suggest;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;
import uk.ac.ebi.uniprot.search.field.QueryBuilder;
import uk.ac.ebi.uniprot.search.field.SuggestField;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.contains;

public class SuggestSearchIT {
    @ClassRule
    public static SuggestSearchEngine searchEngine = new SuggestSearchEngine();
    private static final String REQUEST_HANDLER = "/search";

    @After
    public void cleanup() {
        QueryResponse queryResponse = getResponse("*:*");

        queryResponse.getResults()
                .forEach(doc -> searchEngine.removeEntry(doc.getFieldValue(SuggestField.Stored.id.name()).toString()));
    }

    @Test
    public void exactMatch() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(id)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void leftPrefixMatchWillHit() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(id)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void leftPrefixAltValueDMelanogasterMatchWillHit() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("D. melanogaster");
        String dict = "randomDictionary";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(id)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void leadingZerosAreIgnored() {
        String nonZeroIdPart = "1234";
        String id = "00000" + nonZeroIdPart;
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(id)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void leadingZerosAreIgnoredWithinId() {
        String nonZeroIdPart = "1234";
        String id = "GO:00000" + nonZeroIdPart;
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(id)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void exactMatchComesFirst() {
        String id = "1234";
        String value = "value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String idLonger = id + "567";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(idLonger)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void exactMatchOfSecondWord() {
        String id = "1234";
        String value = "one two three four";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String idLonger = id + "567";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(idLonger)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void prefixMatchOfSecondWord() {
        String id = "1234";
        String value = "one twoooo three four";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String idLonger = id + "567";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(idLonger)
                                        .dictionary(dict)
                                        .value(value)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void idHasPrecedenceOverValue() {
        String id = "12345678";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String someId = "some id";
        String someValue = "some value";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(someId)
                                        .dictionary(dict)
                                        .value(id)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void multiWordIdHasPrecedenceOverValue() {
        String id = "1234 5678";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String someId = "some id";
        String someValue = "some value";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(someId)
                                        .dictionary(dict)
                                        .value(id)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void findsECTypeId() {
        String id = "1.2.3.1";
        String someValue = "some value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        searchEngine.indexEntry(SuggestDocument.builder()
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
    public void findsPrefixECTypeId() {
        String prefixId = "1.2";
        String id = prefixId + "3.1";
        String someValue = "some value";
        List<String> altValue = singletonList("altValue");
        String dict = "randomDictionary";
        String someId = "some id";
        searchEngine.indexEntry(SuggestDocument.builder()
                                        .id(id)
                                        .dictionary(dict)
                                        .value(someValue)
                                        .altValues(altValue)
                                        .build());
        searchEngine.indexEntry(SuggestDocument.builder()
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

    private void checkResultsContains(SolrDocumentList results, int position, String id, String value, List<String> altValues) {
        SolrDocument document = results.get(position);
        checkFieldForDocument(document, SuggestField.Stored.id, id);
        checkFieldForDocument(document, SuggestField.Stored.value, value);
        checkFieldForDocument(document, SuggestField.Stored.altValue, altValues);
    }

    private void checkFieldForDocument(SolrDocument document, SuggestField.Stored valueEnum, String value) {
        Collection<String> fieldNames = document.getFieldNames();

        if (value == null) {
            assertThat(fieldNames, not(contains(valueEnum.name())));
        } else {
            assertThat(document.getFieldValue(valueEnum.name()), is(value));
        }
    }

    private void checkFieldForDocument(SolrDocument document, SuggestField.Stored valueEnum, List<String> values) {
        Collection<String> fieldNames = document.getFieldNames();

        if (values == null) {
            assertThat(fieldNames, not(contains(valueEnum.name())));
        } else {
            assertThat(document.getFieldValue(valueEnum.name()), is(values));
        }
    }

    private String query(String dict, String content) {
        String query = "\"" + content + "\"" + " +" + QueryBuilder.query(SuggestField.Search.dict.name(), dict);
        System.out.println(query);
        return query;
    }
}

