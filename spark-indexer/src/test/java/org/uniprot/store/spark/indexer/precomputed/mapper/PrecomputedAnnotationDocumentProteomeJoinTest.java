package org.uniprot.store.spark.indexer.precomputed.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

class PrecomputedAnnotationDocumentProteomeJoinTest {

    private final PrecomputedAnnotationDocumentProteomeJoin mapper =
            new PrecomputedAnnotationDocumentProteomeJoin();

    @Test
    void canAddProteomesToDocument() {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder().accession("UPI0000001866-61156").build();

        PrecomputedAnnotationDocument result =
                mapper.call(
                        new Tuple2<>(
                                document,
                                Optional.of(List.of("UP000000002", "UP000000001", "UP000000002"))));

        assertEquals(List.of("UP000000001", "UP000000002"), result.getProteome());
    }

    @Test
    void missingProteomeIdsThrowsException() {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder().accession("UPI0000001866-61156").build();

        assertThrows(
                SparkIndexException.class,
                () -> mapper.call(new Tuple2<>(document, Optional.empty())));
    }
}
