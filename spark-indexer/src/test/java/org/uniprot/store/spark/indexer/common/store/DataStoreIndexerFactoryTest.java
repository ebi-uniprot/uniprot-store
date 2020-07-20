package org.uniprot.store.spark.indexer.common.store;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.uniparc.UniParcDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefLightDataStoreIndexer;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class DataStoreIndexerFactoryTest {

    @Test
    void createUniParcDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIPARC, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniParcDataStoreIndexer);
    }

    @Test
    void createUniProtKBDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIPROT, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniProtKBDataStoreIndexer);
    }

    @Test
    void createUniRefDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIREF, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniRefDataStoreIndexer);
    }

    @Test
    void createUniRefLightDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIREF_LIGHT, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniRefLightDataStoreIndexer);
    }
}
