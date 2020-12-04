package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author lgonzales
 * @since 04/12/2020
 */
class SolrIndexParameterTest {

    @Test
    void canBuildSolrIndexParameter() {
        SolrIndexParameter parameter =
                SolrIndexParameter.builder()
                        .collectionName("collectionName")
                        .zkHost("zkHost")
                        .delay(10)
                        .maxRetry(10)
                        .build();
        assertNotNull(parameter);
    }

    @Test
    void canGetSolrIndexParameter() {
        SolrIndexParameter parameter =
                SolrIndexParameter.builder()
                        .collectionName("collectionName")
                        .zkHost("zkHost")
                        .delay(10)
                        .maxRetry(10)
                        .build();
        assertNotNull(parameter);
        assertEquals("collectionName", parameter.getCollectionName());
        assertEquals("zkHost", parameter.getZkHost());
        assertEquals(10, parameter.getDelay());
        assertEquals(10, parameter.getMaxRetry());
    }
}
