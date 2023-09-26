package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
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
 * @since 08/05/2020
 */
class DocumentsToHPSWriterFactoryTest {

    @ParameterizedTest
    @MethodSource("provideSparkCollection")
    void testCreateHPSWriter(SolrCollection collection) {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
        DocumentsToHPSWriter writer = factory.createDocumentsToHPSWriter(collection, jobParameter);
        assertNotNull(writer);
        switch (collection) {
            case uniparc:
                assertTrue(writer instanceof UniParcDocumentsToHPSWriter);
                break;
            case uniprot:
                assertTrue(writer instanceof UniProtKBDocumentsToHPSWriter);
                break;
            case uniref:
                assertTrue(writer instanceof UniRefDocumentsToHPSWriter);
                break;
            case genecentric:
                assertTrue(writer instanceof GeneCentricDocumentsToHPSWriter);
                break;
            case suggest:
                assertTrue(writer instanceof SuggestDocumentsToHPSWriter);
                break;
            case publication:
                assertTrue(writer instanceof PublicationDocumentsToHPSWriter);
                break;
            case literature:
                assertTrue(writer instanceof LiteratureDocumentsToHPSWriter);
                break;
            case taxonomy:
                assertTrue(writer instanceof TaxonomyDocumentsToHPSWriter);
                break;
            case subcellularlocation:
                assertTrue(writer instanceof SubcellularLocationDocumentsToHPSWriter);
                break;
        }
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

    private static Stream<Arguments> provideSparkCollection() {
        return Stream.of(
                Arguments.of(SolrCollection.uniprot),
                Arguments.of(SolrCollection.uniparc),
                Arguments.of(SolrCollection.uniref),
                Arguments.of(SolrCollection.genecentric),
                Arguments.of(SolrCollection.suggest),
                Arguments.of(SolrCollection.publication),
                Arguments.of(SolrCollection.literature),
                Arguments.of(SolrCollection.taxonomy),
                Arguments.of(SolrCollection.subcellularlocation));
    }
}
