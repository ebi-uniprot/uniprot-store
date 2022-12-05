package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;

/**
 * @author lgonzales
 * @since 03/09/2020
 */
class SolrIndexWriterTest {

    @Test
    void canCallThreeBatchesWriter() {
        List<SolrInputDocument> docs = new ArrayList<>();
        docs.add(new SolrInputDocument());
        docs.add(new SolrInputDocument());
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
        FakeSolrIndexWriter writer = new FakeSolrIndexWriter(parameter, docs, false);
        assertDoesNotThrow(() -> writer.call(iterator));
        assertEquals(3, writer.getNumberOfBatchIterations());
    }

    @Test
    void canCallTwoBatchesWriter() {
        List<SolrInputDocument> docs = new ArrayList<>();
        docs.add(new SolrInputDocument());
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
                        .batchSize(3)
                        .build();
        FakeSolrIndexWriter writer = new FakeSolrIndexWriter(parameter, docs, false);
        assertDoesNotThrow(() -> writer.call(iterator));
        assertEquals(2, writer.getNumberOfBatchIterations());
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
        FakeSolrIndexWriter writer = new FakeSolrIndexWriter(parameter, docs, false);
        assertThrows(SolrIndexException.class, () -> writer.call(iterator));
        assertEquals(0, writer.getNumberOfBatchIterations());
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
        FakeSolrIndexWriter writer = new FakeSolrIndexWriter(parameter, new ArrayList<>(), false);
        assertDoesNotThrow(() -> writer.call(iterator));
        assertEquals(0, writer.getNumberOfBatchIterations());
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
        private final SolrClient solrClient;

        public FakeSolrIndexWriter(
                SolrIndexParameter parameter,
                List<SolrInputDocument> docs,
                boolean throwException) {
            super(parameter);
            this.docs = docs;
            this.throwException = throwException;
            this.solrClient = Mockito.mock(SolrClient.class);
        }

        @Override
        protected SolrClient getSolrClient() {
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

        int getNumberOfBatchIterations() {
            int addCall = 0;
            Collection<Invocation> invocations =
                    Mockito.mockingDetails(solrClient).getInvocations();
            for (Invocation invocation : invocations) {
                if (invocation.getMethod().getName().equals("add")) {
                    addCall++;
                }
            }
            return addCall;
        }
    }
}
