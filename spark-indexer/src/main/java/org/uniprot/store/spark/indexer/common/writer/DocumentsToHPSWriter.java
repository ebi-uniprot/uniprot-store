package org.uniprot.store.spark.indexer.common.writer;

/**
 * This class is responsible to prepare solr documents and write it to HPS.
 *
 * @author lgonzales
 * @since 2020-02-27
 */
public interface DocumentsToHPSWriter {

    void writeIndexDocumentsToHPS();
}
