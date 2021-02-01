package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

/**
 * @author lgonzales
 * @since 22/01/2021
 */
class UniParcDataStoreWriterTest {

    @Test
    void canGetDataStoreClient() {
        DataStoreParameter parameter =
                DataStoreParameter.builder()
                        .maxRetry(1)
                        .delay(1)
                        .connectionURL("tcp://localhost")
                        .numberOfConnections(5)
                        .storeName("uniparc")
                        .build();
        UniParcDataStoreWriter writer = new UniParcDataStoreWriter(parameter);
        assertThrows(IllegalArgumentException.class, writer::getDataStoreClient);
    }
}
