package org.uniprot.store.spark.indexer.go.relations;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GOTermFileReaderTest {

    @Test
    void testReadValidGoTermsFile() {
        GOTermFileReader termFileReader = new GOTermFileReader("2020_02/go", null);

        List<GeneOntologyEntry> goTermList = termFileReader.read();

        assertNotNull(goTermList);
        assertEquals(7, goTermList.size(), "Number of expected terms read from test go terms");

        GeneOntologyEntry validGoTerm = new GeneOntologyEntryBuilder().id("GO:0000001").build();
        assertTrue(goTermList.contains(validGoTerm), "Valid go term not found");

        GeneOntologyEntry obsoleteGoTerm = new GeneOntologyEntryBuilder().id("GO:0000008").build();
        assertFalse(goTermList.contains(obsoleteGoTerm), "Obsolete go term found");
    }

    @Test
    void testReadInvalidGoTermsFile() {
        GOTermFileReader termFileReader = new GOTermFileReader("invalid", null);
        assertThrows(
                RuntimeException.class,
                termFileReader::read,
                "IOException loading file: invalid/GO.terms");
    }
}
