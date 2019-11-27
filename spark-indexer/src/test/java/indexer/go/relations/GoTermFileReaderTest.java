package indexer.go.relations;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GoTermFileReaderTest {

    @Test
    void testReadValidGoTermsFile() {
        GoTermFileReader termFileReader = new GoTermFileReader("go", null);

        List<GoTerm> goTermList = termFileReader.read();

        assertNotNull(goTermList);
        assertEquals(747, goTermList.size(), "Number of expected terms read from test go terms");

        GoTerm validGoTerm = new GoTermImpl("GO:0015464", null);
        assertTrue(goTermList.contains(validGoTerm), "Valid go term not found");

        GoTerm obsoleteGoTerm = new GoTermImpl("GO:0015465", null);
        assertFalse(goTermList.contains(obsoleteGoTerm), "Obsolete go term found");
    }

    @Test
    void testReadInvalidGoTermsFile() {
        GoTermFileReader termFileReader = new GoTermFileReader("invalid", null);
        assertThrows(RuntimeException.class, termFileReader::read, "IOException loading file: invalid/GO.terms");
    }

}