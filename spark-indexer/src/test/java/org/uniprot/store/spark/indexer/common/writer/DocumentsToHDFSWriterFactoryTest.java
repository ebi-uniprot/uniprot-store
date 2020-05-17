package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.suggest.SuggestDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniparc.UniParcDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniref.UniRefDocumentsToHDFSWriter;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class DocumentsToHDFSWriterFactoryTest {

    @Test
    void createUniParcDocumentsToHDFSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHDFSWriterFactory factory = new DocumentsToHDFSWriterFactory();
        DocumentsToHDFSWriter writer =
                factory.createDocumentsToHDFSWriter(SolrCollection.uniparc, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof UniParcDocumentsToHDFSWriter);
    }

    @Test
    void createUniProtKBDocumentsToHDFSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHDFSWriterFactory factory = new DocumentsToHDFSWriterFactory();
        DocumentsToHDFSWriter writer =
                factory.createDocumentsToHDFSWriter(SolrCollection.uniprot, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof UniProtKBDocumentsToHDFSWriter);
    }

    @Test
    void createUniRefDocumentsToHDFSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHDFSWriterFactory factory = new DocumentsToHDFSWriterFactory();
        DocumentsToHDFSWriter writer =
                factory.createDocumentsToHDFSWriter(SolrCollection.uniref, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof UniRefDocumentsToHDFSWriter);
    }

    @Test
    void createSuggestDocumentsToHDFSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHDFSWriterFactory factory = new DocumentsToHDFSWriterFactory();
        DocumentsToHDFSWriter writer =
                factory.createDocumentsToHDFSWriter(SolrCollection.suggest, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof SuggestDocumentsToHDFSWriter);
    }

    @Test
    void createNotSupportedCollectionThrowsException() {
        DocumentsToHDFSWriterFactory factory = new DocumentsToHDFSWriterFactory();
        assertThrows(
                UnsupportedOperationException.class,
                () -> factory.createDocumentsToHDFSWriter(SolrCollection.crossref, null));
    }
}
