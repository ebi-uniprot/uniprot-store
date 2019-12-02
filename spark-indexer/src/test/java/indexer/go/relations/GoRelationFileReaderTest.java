package indexer.go.relations;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GoRelationFileReaderTest {

    @Test
    void testReadValidGoRelationsFile() {
        GoRelationFileReader relationFileReader = new GoRelationFileReader("go", null);

        Map<String, Set<String>> goRelations = relationFileReader.read();

        assertNotNull(goRelations);
        assertEquals(
                881,
                goRelations.size(),
                "Number of expected relations read from test go relation file");

        assertNotNull(goRelations.get("GO:0006853"), "Has a valid relation");
        Set<String> relationSet = goRelations.get("GO:0006853");
        assertEquals(4, relationSet.size(), "Expected Number of relations of GO:0006853");

        assertTrue(relationSet.contains("GO:1990542"));
        assertTrue(relationSet.contains("GO:0015909"));
        assertTrue(relationSet.contains("GO:1902001"));
        assertTrue(relationSet.contains("GO:0032365"));
    }

    @Test
    void testReadInvalidGoRelationsFile() {
        GoRelationFileReader relationFileReader = new GoRelationFileReader("invalid", null);
        assertThrows(
                RuntimeException.class,
                relationFileReader::read,
                "IOException loading file: invalid/GO.relations");
    }
}
