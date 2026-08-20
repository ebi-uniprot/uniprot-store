package org.uniprot.store.spark.indexer.common.util;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.keyword.KeywordDocument;

/**
 * @author lgonzales
 * @since 29/09/2020
 */
class SolrUtilsTest {

    @Test
    void convertToSolrInputDocumentSuccess() {
        KeywordDocument doc = KeywordDocument.builder().id("KW-12345").name("name").build();

        SolrInputDocument solrInput = SolrUtils.convertToSolrInputDocument(doc);
        assertNotNull(solrInput);
        assertEquals(8, solrInput.values().size());
        assertNotNull(solrInput.getFieldValue("id"));
        assertEquals("KW-12345", solrInput.getFieldValue("id"));
    }

    @Test
    void convertToSolrInputDocumentNull() {
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> SolrUtils.convertToSolrInputDocument(null));

        assertEquals("Cannot convert null document to SolrInputDocument", exception.getMessage());
    }

    @Test
    void commit() {
        assertDoesNotThrow(() -> SolrUtils.commit("uniprot", "myHost:2191"));
    }
}
