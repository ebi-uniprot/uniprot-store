package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;

/**
 * @author lgonzales
 * @since 03/09/2020
 */
class SolrIndexWriterTest {

    @Test
    void canCallWriter() {
        List<SolrInputDocument> docs = new ArrayList<>();
        docs.add(new SolrInputDocument());
        docs.add(new SolrInputDocument());
        docs.add(new SolrInputDocument());
        Iterator<SolrInputDocument> iterator = docs.iterator();
        SolrIndexParameter parameter =
                SolrIndexParameter.builder()
                        .zkHost("zkHost")
                        .collectionName("collectionName")
                        .delay(1L)
                        .maxRetry(1)
                        .batchSize(2)
                        .build();
        SolrIndexWriter writer = new FakeSolrIndexWriter(parameter, docs, false);
        assertDoesNotThrow(() -> writer.call(iterator));
    }

    @Test
    void canCallWriterWrongBatchSize() {
        List<SolrInputDocument> docs = new ArrayList<>();
        docs.add(new SolrInputDocument());
        docs.add(new SolrInputDocument());
        docs.add(new SolrInputDocument());
        Iterator<SolrInputDocument> iterator = docs.iterator();
        SolrIndexParameter parameter =
                SolrIndexParameter.builder()
                        .zkHost("zkHost")
                        .collectionName("collectionName")
                        .delay(1L)
                        .maxRetry(1)
                        .batchSize(0)
                        .build();
        SolrIndexWriter writer = new FakeSolrIndexWriter(parameter, docs, false);
        assertThrows(SolrIndexException.class, () -> writer.call(iterator));
    }


    @Test
    void canCallWriterEmptyList() {
        Iterator<SolrInputDocument> iterator = Collections.emptyIterator();
        SolrIndexParameter parameter =
                SolrIndexParameter.builder()
                        .zkHost("zkHost")
                        .collectionName("collectionName")
                        .delay(1L)
                        .maxRetry(1)
                        .batchSize(1)
                        .build();
        SolrIndexWriter writer = new FakeSolrIndexWriter(parameter, new ArrayList<>(), false);
        assertDoesNotThrow(() -> writer.call(iterator));
    }
    @Test
    void callWriterFail() throws Exception {
        List<SolrInputDocument> docs = new ArrayList<>();
        docs.add(new SolrInputDocument());
        Iterator<SolrInputDocument> iterator = docs.iterator();

        SolrIndexParameter parameter =
                SolrIndexParameter.builder()
                        .zkHost("zkHost")
                        .collectionName("collectionName")
                        .delay(1L)
                        .maxRetry(1)
                        .build();
        SolrIndexWriter writer = new FakeSolrIndexWriter(parameter, docs, true);
        SolrIndexException response =
                assertThrows(SolrIndexException.class, () -> writer.call(iterator));
        assertEquals(
                "Exception indexing data to Solr, for collection collectionName",
                response.getMessage());
    }

    private static class FakeSolrIndexWriter extends SolrIndexWriter {
        private static final long serialVersionUID = 7351466400932705045L;
        private final boolean throwException;
        private final List<SolrInputDocument> docs;

        public FakeSolrIndexWriter(
                SolrIndexParameter parameter,
                List<SolrInputDocument> docs,
                boolean throwException) {
            super(parameter);
            this.docs = docs;
            this.throwException = throwException;
        }

        @Override
        protected SolrClient getSolrClient() {
            SolrClient solrClient = Mockito.mock(SolrClient.class);
            try {
                if (throwException) {
                    Mockito.when(solrClient.add(Mockito.anyString(), Mockito.eq(docs)))
                            .thenThrow(new SolrServerException("ErrorMessage"));
                } else {
                    Mockito.when(solrClient.add(Mockito.anyString(), Mockito.eq(docs)))
                            .thenReturn(null);
                }
            } catch (Exception e) {
                Assertions.fail(e.getMessage());
            }
            return solrClient;
        }
    }
}
