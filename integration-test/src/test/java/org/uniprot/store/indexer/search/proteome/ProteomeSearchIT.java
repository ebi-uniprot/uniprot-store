package org.uniprot.store.indexer.search.proteome;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.json.parser.proteome.ProteomeJsonConfig;
import org.uniprot.core.xml.XmlChainIterator;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtSearchFields;

import com.fasterxml.jackson.databind.ObjectMapper;

class ProteomeSearchIT {
    private static final String PROTEOME_ROOT_ELEMENT = "proteome";
    @RegisterExtension static ProteomeSearchEngine searchEngine = new ProteomeSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() {
        List<String> files = Collections.singletonList("it/proteome/proteome_example.xml");

        XmlChainIterator<Proteome, Proteome> chainingIterators =
                new XmlChainIterator<>(
                        new XmlChainIterator.FileInputStreamIterator(files),
                        Proteome.class,
                        PROTEOME_ROOT_ELEMENT,
                        Function.identity());

        new XmlChainIterator<>(
                new XmlChainIterator.FileInputStreamIterator(files),
                Proteome.class,
                PROTEOME_ROOT_ELEMENT,
                Function.identity());

        while (chainingIterators.hasNext()) {
            Proteome next = chainingIterators.next();
            searchEngine.indexEntry(next);
        }
    }

    @Test
    void searchUPid() {
        String upid = "UP000029775";
        String query = upid(upid);
        QueryResponse queryResponse = searchEngine.getQueryResponse(query);

        SolrDocumentList results = queryResponse.getResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0).containsValue(upid));
    }

    @Test
    void searchAllUPid() {
        String upid = "*";
        String query = upid(upid);
        QueryResponse queryResponse = searchEngine.getQueryResponse(query);

        SolrDocumentList results = queryResponse.getResults();
        assertEquals(6, results.size());
    }

    @Test
    void searchListUPid() {
        List<String> upids = Arrays.asList("UP000029775", "UP000029766", "UP000000718");

        String query = upid(upids.get(0));
        for (int i = 1; i < upids.size(); i++) {
            query = QueryBuilder.or(query, upid(upids.get(i)));
        }

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);

        SolrDocumentList results = queryResponse.getResults();
        assertEquals(3, results.size());
        List<String> foundUpids =
                StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(
                                        results.listIterator(), Spliterator.ORDERED),
                                false)
                        .map(val -> val.getFieldValue("upid"))
                        .map(val -> (String) val)
                        .collect(Collectors.toList());

        assertTrue(foundUpids.contains("UP000000718"));

        assertTrue(foundUpids.contains("UP000029766"));

        assertTrue(foundUpids.contains("UP000029775"));
    }

    @Test
    void searchTaxId() {
        int taxId = 60714;
        String query = taxonomy(taxId);

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);

        SolrDocumentList results = queryResponse.getResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0).containsValue("UP000029766"));
    }

    @Test
    void searchIsRedundant() {

        String query = isRedudant(false);

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);

        SolrDocumentList results = queryResponse.getResults();
        assertEquals(6, results.size());
    }

    @Test
    void fetchAvroObject() {
        String upid = "UP000000718";
        String query = upid(upid);

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);

        SolrDocumentList results = queryResponse.getResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0).containsValue("UP000000718"));
        DocumentObjectBinder binder = new DocumentObjectBinder();
        ProteomeDocument proteomeDoc = binder.getBean(ProteomeDocument.class, results.get(0));
        org.uniprot.core.proteome.ProteomeEntry proteome = toProteome(proteomeDoc);
        assertNotNull(proteome);
        assertEquals("UP000000718", proteome.getId().getValue());
    }

    org.uniprot.core.proteome.ProteomeEntry toProteome(ProteomeDocument proteomeDoc) {
        try {
            ObjectMapper objectMapper = ProteomeJsonConfig.getInstance().getFullObjectMapper();
            return objectMapper.readValue(
                    proteomeDoc.proteomeStored.array(),
                    org.uniprot.core.proteome.ProteomeEntry.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String upid(String upid) {
        return QueryBuilder.query(UniProtSearchFields.PROTEOME.getField("upid").getName(), upid);
    }

    private String taxonomy(int taxId) {
        return QueryBuilder.query(
                UniProtSearchFields.PROTEOME.getField("taxonomy_id").getName(), "" + taxId);
    }

    private String isRedudant(Boolean b) {

        return QueryBuilder.query(
                UniProtSearchFields.PROTEOME.getField("redundant").getName(), b.toString());
    }
}
