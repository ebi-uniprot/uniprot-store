package org.uniprot.store.spark.indexer.precomputed.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

class PrecomputedAnnotationEntryToDocumentMapperTest {

    private PrecomputedAnnotationEntryToDocumentMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new PrecomputedAnnotationEntryToDocumentMapper();
    }

    @Test
    void canMapPrecomputedEntryToDocument() throws Exception {
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(
                                "UPI0000001866-61156",
                                "UPI0000001866_61156",
                                UniProtKBEntryType.TREMBL)
                        .build();

        PrecomputedAnnotationDocument document = mapper.call(entry);

        assertEquals("UPI0000001866-61156", document.getAccession());
        assertEquals("UPI0000001866", document.getUniparc());
        assertEquals(61156, document.getTaxonomyId());
        assertEquals("UPI0000001866-61156", document.getDocumentId());
        assertTrue(document.getProteome().isEmpty());
    }
}
