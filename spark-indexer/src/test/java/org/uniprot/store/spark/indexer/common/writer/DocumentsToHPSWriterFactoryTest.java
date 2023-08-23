package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.proteome.ProteomeDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.suggest.SuggestDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.uniparc.UniParcDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.uniref.UniRefDocumentsToHPSWriter;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class DocumentsToHPSWriterFactoryTest {

    @Test
    void createUniParcDocumentsToHPSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
        DocumentsToHPSWriter writer =
                factory.createDocumentsToHPSWriter(SolrCollection.uniparc, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof UniParcDocumentsToHPSWriter);
    }

    @Test
    void createUniProtKBDocumentsToHPSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
        DocumentsToHPSWriter writer =
                factory.createDocumentsToHPSWriter(SolrCollection.uniprot, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof UniProtKBDocumentsToHPSWriter);
    }

    @Test
    void createUniRefDocumentsToHPSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
        DocumentsToHPSWriter writer =
                factory.createDocumentsToHPSWriter(SolrCollection.uniref, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof UniRefDocumentsToHPSWriter);
    }

    @Test
    void createSuggestDocumentsToHPSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
        DocumentsToHPSWriter writer =
                factory.createDocumentsToHPSWriter(SolrCollection.suggest, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof SuggestDocumentsToHPSWriter);
    }

    @Test
    void createProteomeDocumentsToHPSWriterFactory() {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
        DocumentsToHPSWriter writer =
                factory.createDocumentsToHPSWriter(SolrCollection.proteome, jobParameter);
        assertNotNull(writer);
        assertTrue(writer instanceof ProteomeDocumentsToHPSWriter);
    }

    @Test
    void createNotSupportedCollectionThrowsException() {
        DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
        assertThrows(
                UnsupportedOperationException.class,
                () -> factory.createDocumentsToHPSWriter(SolrCollection.crossref, null));
    }
}
