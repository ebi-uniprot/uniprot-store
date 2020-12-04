package org.uniprot.store.spark.indexer.common.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 03/12/2020
 */
class DataStoreParameterTest {

    @Test
    void canBuildDataStoreParameter() {
        DataStoreParameter parameter = DataStoreParameter.builder()
                .storeName("storeName")
                .connectionURL("connectionURL")
                .numberOfConnections(5)
                .delay(10)
                .maxRetry(10)
                .build();
        assertNotNull(parameter);
    }

    @Test
    void canGetDataStoreParameter() {
        DataStoreParameter parameter = DataStoreParameter.builder()
                .storeName("storeName")
                .connectionURL("connectionURL")
                .numberOfConnections(5)
                .delay(10)
                .maxRetry(10)
                .build();
        assertNotNull(parameter);
        assertEquals("storeName", parameter.getStoreName());
        assertEquals("connectionURL", parameter.getConnectionURL());
        assertEquals(5, parameter.getNumberOfConnections());
        assertEquals(10, parameter.getDelay());
        assertEquals(10, parameter.getMaxRetry());
    }

}