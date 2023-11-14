package org.uniprot.store.spark.indexer.validator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.validator.impl.*;

class SolrIndexValidatorFactoryTest {

    @ParameterizedTest
    @MethodSource("provideSparkCollection")
    void testCreateHPSWriter(SolrCollection collection) {
        JobParameter jobParameter = Mockito.mock(JobParameter.class);
        SolrIndexValidatorFactory factory = new SolrIndexValidatorFactory();
        SolrIndexValidator validator = factory.createSolrIndexValidator(collection, jobParameter);
        assertNotNull(validator);
        switch (collection) {
            case uniparc:
                assertTrue(validator instanceof UniParcSolrIndexValidator);
                break;
            case uniprot:
                assertTrue(validator instanceof UniProtKBSolrIndexValidator);
                break;
            case uniref:
                assertTrue(validator instanceof UniRefSolrIndexValidator);
                break;
            case genecentric:
                assertTrue(validator instanceof GeneCentricSolrIndexValidator);
                break;
            case publication:
                assertTrue(validator instanceof PublicationSolrIndexValidator);
                break;
            case literature:
                assertTrue(validator instanceof LiteratureSolrIndexValidator);
                break;
            case taxonomy:
                assertTrue(validator instanceof TaxonomySolrIndexValidator);
                break;
            case subcellularlocation:
                assertTrue(validator instanceof SubcellularLocationSolrIndexValidator);
                break;
        }
    }

    @Test
    void createNotSupportedCollectionThrowsException() {
        SolrIndexValidatorFactory factory = new SolrIndexValidatorFactory();
        assertThrows(
                UnsupportedOperationException.class,
                () -> factory.createSolrIndexValidator(SolrCollection.suggest, null));
    }

    private static Stream<Arguments> provideSparkCollection() {
        return Stream.of(
                Arguments.of(SolrCollection.uniprot),
                Arguments.of(SolrCollection.uniparc),
                Arguments.of(SolrCollection.uniref),
                Arguments.of(SolrCollection.genecentric),
                Arguments.of(SolrCollection.publication),
                Arguments.of(SolrCollection.literature),
                Arguments.of(SolrCollection.taxonomy),
                Arguments.of(SolrCollection.subcellularlocation));
    }
}
