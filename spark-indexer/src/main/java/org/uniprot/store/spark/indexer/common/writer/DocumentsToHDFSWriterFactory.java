package org.uniprot.store.spark.indexer.common.writer;

import java.util.ResourceBundle;

import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.suggest.SuggestDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniparc.UniParcDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniref.UniRefDocumentsToHDFSWriter;

/**
 * @author lgonzales
 * @since 2020-02-27
 */
public class DocumentsToHDFSWriterFactory {

    public DocumentsToHDFSWriter createDocumentsToHDFSWriter(SolrCollection collection) {
        DocumentsToHDFSWriter writer = null;
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        switch (collection) {
            case uniprot:
                writer = new UniProtKBDocumentsToHDFSWriter(applicationConfig);
                break;
            case suggest:
                writer = new SuggestDocumentsToHDFSWriter(applicationConfig);
                break;
            case uniref:
                writer = new UniRefDocumentsToHDFSWriter(applicationConfig);
                break;
            case uniparc:
                writer = new UniParcDocumentsToHDFSWriter(applicationConfig);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Collection not yet supported by spark indexer");
        }
        return writer;
    }
}
