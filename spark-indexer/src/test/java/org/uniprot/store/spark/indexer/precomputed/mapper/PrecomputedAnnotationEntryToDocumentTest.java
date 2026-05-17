package org.uniprot.store.spark.indexer.precomputed.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

class PrecomputedAnnotationEntryToDocumentTest {

    private final PrecomputedAnnotationEntryToDocument mapper =
            new PrecomputedAnnotationEntryToDocument();

    @Test
    void canMapFlatFileEntryToDocument() {
        String entry =
                "AC   UPI0000001866-61156;\n"
                        + "DE   RecName: Full=ADP/ATP translocase;\n"
                        + "//\n";

        Tuple2<String, PrecomputedAnnotationDocument> result = mapper.call(entry);
        PrecomputedAnnotationDocument document = result._2;

        assertEquals("61156", result._1);
        assertEquals("UPI0000001866-61156", document.getAccession());
    }

    @Test
    void missingAccessionThrowsException() {
        String entry = "DE   RecName: Full=ADP/ATP translocase;\n//\n";

        assertThrows(SparkIndexException.class, () -> mapper.call(entry));
    }

    @Test
    void missingTaxonomyInAccessionThrowsException() {
        String entry = "AC   UPI0000001866;\n//\n";

        assertThrows(SparkIndexException.class, () -> mapper.call(entry));
    }

    @Test
    void canAddProteomesToDocument() {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder().accession("UPI0000001866-61156").build();

        PrecomputedAnnotationDocument result =
                PrecomputedAnnotationEntryToDocument.withProteomes(
                        document, List.of("UP000000002", "UP000000001", "UP000000002"));

        assertEquals(List.of("UP000000001", "UP000000002"), result.getProteome());
    }
}
