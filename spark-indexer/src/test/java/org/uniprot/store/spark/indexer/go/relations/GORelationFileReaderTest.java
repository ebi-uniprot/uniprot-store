package org.uniprot.store.spark.indexer.go.relations;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GORelationFileReaderTest {

    @Test
    void testReadValidGoRelationsFile() {
        GORelationFileReader relationFileReader = new GORelationFileReader("2020_02/go", null);

        Map<String, Set<String>> goRelations = relationFileReader.read();

        assertNotNull(goRelations);
        assertEquals(
                7,
                goRelations.size(),
                "Number of expected relations read from test go relation file");

        assertNotNull(goRelations.get("GO:0000010"), "Has a valid relation");
        Set<String> relationSet = goRelations.get("GO:0000001");
        assertEquals(2, relationSet.size(), "Expected Number of relations of GO:0006853");

        assertTrue(relationSet.contains("GO:0048311"));
        assertTrue(relationSet.contains("GO:0048308"));
    }

    @Test
    void testReadInvalidGoRelationsFile() {
        GORelationFileReader relationFileReader = new GORelationFileReader("invalid", null);
        assertThrows(
                RuntimeException.class,
                relationFileReader::read,
                "IOException loading file: invalid/GO.relations");
    }
}
