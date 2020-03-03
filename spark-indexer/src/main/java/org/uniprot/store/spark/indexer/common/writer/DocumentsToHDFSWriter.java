package org.uniprot.store.spark.indexer.common.writer;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author lgonzales
 * @since 2020-02-27
 */
public interface DocumentsToHDFSWriter {

    void writeIndexDocumentsToHDFS(JavaSparkContext sparkContext, String releaseName);
}
