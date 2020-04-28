package org.uniprot.store.spark.indexer.common.writer;

/**
 * @author lgonzales
 * @since 2020-02-27
 */
public interface DocumentsToHDFSWriter {

    void writeIndexDocumentsToHDFS();
}
