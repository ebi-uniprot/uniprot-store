package org.uniprot.store.spark.indexer.common.store;

/**
 * This class is responsible to prepare data store entities and save it to dataStore.
 *
 * @author lgonzales
 * @since 27/04/2020
 */
public interface DataStoreIndexer {
    String BROTLI_COMPRESSION_ENABLED = "brotli.compression.enabled";
    String BROTLI_COMPRESSION_LEVEL = "brotli.compression.level";
    void indexInDataStore();
}
