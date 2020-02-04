package org.uniprot.store.spark.indexer.go.relations;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GOTermFileReaderTest {

    @Test
    void testReadValidGoTermsFile() {
        GOTermFileReader termFileReader = new GOTermFileReader("go", null);

        List<GOTerm> goTermList = termFileReader.read();

        assertNotNull(goTermList);
        assertEquals(747, goTermList.size(), "Number of expected terms read from test go terms");

        GOTerm validGoTerm = new GOTermImpl("GO:0015464", null);
        assertTrue(goTermList.contains(validGoTerm), "Valid go term not found");

        GOTerm obsoleteGoTerm = new GOTermImpl("GO:0015465", null);
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
