package org.uniprot.store.spark.indexer.common.writer;

import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.genecentric.GeneCentricDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.literature.LiteratureDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.proteome.ProteomeDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.subcellularlocation.SubcellularLocationDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.suggest.SuggestDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.uniparc.UniParcDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.uniref.UniRefDocumentsToHPSWriter;

/**
 * @author lgonzales
 * @since 2020-02-27
 */
public class DocumentsToHPSWriterFactory {

    public DocumentsToHPSWriter createDocumentsToHPSWriter(
            SolrCollection collection, JobParameter jobParameter) {
        DocumentsToHPSWriter writer = null;
        switch (collection) {
            case genecentric:
                writer = new GeneCentricDocumentsToHPSWriter(jobParameter);
                break;
            case literature:
                writer = new LiteratureDocumentsToHPSWriter(jobParameter);
                break;
            case proteome:
                writer = new ProteomeDocumentsToHPSWriter(jobParameter);
                break;
            case publication:
                writer = new PublicationDocumentsToHPSWriter(jobParameter);
                break;
            case subcellularlocation:
                writer = new SubcellularLocationDocumentsToHPSWriter(jobParameter);
                break;
            case suggest:
                writer = new SuggestDocumentsToHPSWriter(jobParameter);
                break;
            case taxonomy:
                writer = new TaxonomyDocumentsToHPSWriter(jobParameter);
                break;
            case uniparc:
                writer = new UniParcDocumentsToHPSWriter(jobParameter);
                break;
            case uniprot:
                writer = new UniProtKBDocumentsToHPSWriter(jobParameter);
                break;
            case uniref:
                writer = new UniRefDocumentsToHPSWriter(jobParameter);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Collection not yet supported by spark indexer");
        }
        return writer;
    }
}
