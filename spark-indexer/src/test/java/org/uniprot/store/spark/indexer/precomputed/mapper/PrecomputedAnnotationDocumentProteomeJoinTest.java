package org.uniprot.store.spark.indexer.precomputed.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

import scala.Tuple2;

class PrecomputedAnnotationDocumentProteomeJoinTest {

    private final PrecomputedAnnotationDocumentProteomeJoin mapper =
            new PrecomputedAnnotationDocumentProteomeJoin();

    @Test
    void canAddProteomesToDocument() {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder()
                        .accession("UPI0000001866-61156")
                        .uniparc("UPI0000001866")
                        .taxonomyId(61156)
                        .build();

        PrecomputedAnnotationDocument result =
                mapper.call(
                        new Tuple2<>(
                                document,
                                Optional.of(List.of("UP000000002", "UP000000001", "UP000000002"))));

        assertEquals(List.of("UP000000002", "UP000000001"), result.getProteome());
        assertEquals("UPI0000001866-61156", result.getAccession());
        assertEquals("UPI0000001866", result.getUniparc());
        assertEquals(61156, result.getTaxonomyId());
    }

    @Test
    void missingProteomeIdsReturnsDocumentWithEmptyProteomes() {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder().accession("UPI0000001866-61156").build();

        PrecomputedAnnotationDocument result =
                mapper.call(new Tuple2<>(document, Optional.empty()));

        assertTrue(result.getProteome().isEmpty());
    }
}
