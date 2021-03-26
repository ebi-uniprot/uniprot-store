package org.uniprot.store.spark.indexer.common.writer;

import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.genecentric.GeneCentricDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.literature.LiteratureDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.suggest.SuggestDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniparc.UniParcDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniref.UniRefDocumentsToHDFSWriter;

/**
 * @author lgonzales
 * @since 2020-02-27
 */
public class DocumentsToHDFSWriterFactory {

    public DocumentsToHDFSWriter createDocumentsToHDFSWriter(
            SolrCollection collection, JobParameter jobParameter) {
        DocumentsToHDFSWriter writer = null;
        switch (collection) {
            case uniprot:
                writer = new UniProtKBDocumentsToHDFSWriter(jobParameter);
                break;
            case suggest:
                writer = new SuggestDocumentsToHDFSWriter(jobParameter);
                break;
            case uniref:
                writer = new UniRefDocumentsToHDFSWriter(jobParameter);
                break;
            case uniparc:
                writer = new UniParcDocumentsToHDFSWriter(jobParameter);
                break;
            case genecentric:
                writer = new GeneCentricDocumentsToHDFSWriter(jobParameter);
                break;
            case publication:
                writer = new PublicationDocumentsToHDFSWriter(jobParameter);
                break;
            case literature:
                writer = new LiteratureDocumentsToHDFSWriter(jobParameter);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Collection not yet supported by spark indexer");
        }
        return writer;
    }
}
