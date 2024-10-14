package org.uniprot.store.spark.indexer.common.store;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.uniparc.UniParcLightDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefLightDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefMembersDataStoreIndexer;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class DataStoreIndexerFactoryTest {

    @Test
    void createUniParcDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIPARC_LIGHT, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniParcLightDataStoreIndexer);
    }

    @Test
    void createUniProtKBDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIPROT, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniProtKBDataStoreIndexer);
    }

    @Test
    void createUniRefLightDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIREF_LIGHT, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniRefLightDataStoreIndexer);
    }

    @Test
    void createUniRefMemberDataStoreIndexer() {
        DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
        DataStoreIndexer indexer = factory.createDataStoreIndexer(DataStore.UNIREF_MEMBER, null);
        assertNotNull(indexer);
        assertTrue(indexer instanceof UniRefMembersDataStoreIndexer);
    }
}
