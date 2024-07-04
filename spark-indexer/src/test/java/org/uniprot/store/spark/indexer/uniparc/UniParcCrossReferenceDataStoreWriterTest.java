package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

class UniParcCrossReferenceDataStoreWriterTest {

    @Test
    void canGetDataStoreClient() {
        DataStoreParameter parameter =
                DataStoreParameter.builder()
                        .maxRetry(1)
                        .delay(1)
                        .connectionURL("tcp://localhost")
                        .numberOfConnections(5)
                        .storeName("cross-reference")
                        .build();
        UniParcCrossReferenceDataStoreWriter writer =
                new UniParcCrossReferenceDataStoreWriter(parameter);
        assertThrows(IllegalArgumentException.class, writer::getDataStoreClient);
    }
}
