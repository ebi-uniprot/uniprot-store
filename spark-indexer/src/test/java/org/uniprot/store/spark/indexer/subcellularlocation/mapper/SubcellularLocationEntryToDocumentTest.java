package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.cv.subcell.SubcellularLocationFileReader;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;

/**
 * @author sahmad
 * @created 03/02/2022
 */
class SubcellularLocationEntryToDocumentTest {

    @Test
    void testConvertEntryToDocument() throws Exception {
        SubcellularLocationFileReader reader = new SubcellularLocationFileReader();
        List<String> input =
                List.of(
                        "_______________________________",
                        "ID   Cell tip.",
                        "AC   SL-0456",
                        "DE   The region at either end of the longest axis of a cylindrical or",
                        "DE   elongated cell, where polarized growth may occur.",
                        "SL   Cell tip.",
                        "GO   GO:0051286; cell tip",
                        "HI   Membrane.",
                        "HP   Acidocalcisome.",
                        "HP   Endomembrane system.",
                        "//");
        List<SubcellularLocationEntry> entries = reader.parseLines(input);
        Assertions.assertNotNull(entries);
        Assertions.assertEquals(1, entries.size());

        SubcellularLocationEntryToDocument entryToDocument =
                new SubcellularLocationEntryToDocument(new HashMap<>());
        SubcellularLocationDocument document = entryToDocument.call(entries.get(0));
        Assertions.assertNotNull(document);
        Assertions.assertEquals("SL-0456", document.getId());
        Assertions.assertEquals("Cell tip", document.getName());
        Assertions.assertNotNull(document.getSubcellularlocationObj());
        Assertions.assertNotNull(document.getDefinition());
        Assertions.assertNotNull(document.getCategory());
    }
}
