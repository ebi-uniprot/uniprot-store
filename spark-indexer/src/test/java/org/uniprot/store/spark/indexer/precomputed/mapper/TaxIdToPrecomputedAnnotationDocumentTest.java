package org.uniprot.store.spark.indexer.precomputed.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

import scala.Tuple2;

class TaxIdToPrecomputedAnnotationDocumentTest {

    private final TaxIdToPrecomputedAnnotationDocument mapper =
            new TaxIdToPrecomputedAnnotationDocument();

    @Test
    void canMapDocumentToTaxonomyIdAndDocument() throws Exception {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder()
                        .accession("UPI0000001866-61156")
                        .uniparc("UPI0000001866")
                        .taxonomyId(61156)
                        .build();

        Tuple2<Integer, PrecomputedAnnotationDocument> result = mapper.call(document);

        assertEquals(61156, result._1);
        assertEquals(document, result._2);
    }
}
