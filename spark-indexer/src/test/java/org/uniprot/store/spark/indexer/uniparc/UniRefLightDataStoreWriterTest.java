package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniref.writer.UniRefLightDataStoreWriter;

class UniRefLightDataStoreWriterTest {

    @Test
    void canGetDataStoreClient() {
        DataStoreParameter parameter =
                DataStoreParameter.builder()
                        .maxRetry(1)
                        .delay(1)
                        .connectionURL("tcp://localhost")
                        .numberOfConnections(5)
                        .storeName("uniparc-light")
                        .build();
        UniRefLightDataStoreWriter writer = new UniRefLightDataStoreWriter(parameter);
        assertThrows(IllegalArgumentException.class, writer::getDataStoreClient);
    }
}
