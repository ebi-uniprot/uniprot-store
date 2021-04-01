package org.uniprot.store.spark.indexer.publication.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.publication.impl.CommunityMappedReferenceBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 01/04/2021
 */
class MappedReferencePairMapperTest {

    @Test
    void mapForAccessionAndCitation() throws Exception {
        MappedReferencePairMapper mapper =
                new MappedReferencePairMapper(KeyType.ACCESSION_AND_CITATION_ID);
        MappedReference mappedReference =
                new CommunityMappedReferenceBuilder()
                        .uniProtKBAccession("P12345")
                        .citationId("555555")
                        .build();
        Tuple2<String, MappedReference> result = mapper.call(mappedReference);
        assertNotNull(result);
        assertEquals("P12345_555555", result._1);
        assertEquals(mappedReference, result._2);
    }

    @Test
    void mapForCitationOnly() throws Exception {
        MappedReferencePairMapper mapper = new MappedReferencePairMapper(KeyType.CITATION_ID);
        MappedReference mappedReference =
                new CommunityMappedReferenceBuilder()
                        .uniProtKBAccession("P12345")
                        .citationId("555555")
                        .build();
        Tuple2<String, MappedReference> result = mapper.call(mappedReference);
        assertNotNull(result);
        assertEquals("555555", result._1);
        assertEquals(mappedReference, result._2);
    }
}
