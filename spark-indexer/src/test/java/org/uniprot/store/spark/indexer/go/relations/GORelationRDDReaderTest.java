package org.uniprot.store.spark.indexer.go.relations;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.Test;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GORelationRDDReaderTest {

    @Test
    void getAncestorsWithAncestors() {
        GOTerm goTerm1 = new GOTermImpl("GO1", "TERM1");
        List<GOTerm> goTermList = new ArrayList<>();
        goTermList.add(goTerm1);
        goTermList.add(new GOTermImpl("GO2", "TERM2"));
        goTermList.add(new GOTermImpl("GO3", "TERM3"));
        goTermList.add(new GOTermImpl("GO4", "TERM4"));
        goTermList.add(new GOTermImpl("GO5", "TERM5"));
        Map<String, Set<String>> relations = new HashMap<>();
        relations.put("GO1", Collections.singleton("GO3"));

        Set<String> go3Relations = new HashSet<>();
        go3Relations.add("GO4");
        go3Relations.add("GO5");
        go3Relations.add("GO1");
        relations.put("GO3", go3Relations);

        Set<GOTerm> goTermRelations =
                GORelationRDDReader.getAncestors(goTerm1, goTermList, relations);
        assertNotNull(goTermRelations);
        assertEquals(4, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GOTermImpl("GO1", null)));
        assertTrue(goTermRelations.contains(new GOTermImpl("GO3", null)));
        assertTrue(goTermRelations.contains(new GOTermImpl("GO4", null)));
        assertTrue(goTermRelations.contains(new GOTermImpl("GO5", null)));
    }

    @Test
    void getAncestorsWithoutAncestors() {
        GOTerm goTerm1 = new GOTermImpl("GO1", "TERM1");
        List<GOTerm> goTermList = Collections.singletonList(goTerm1);
        Set<GOTerm> goTermRelations =
                GORelationRDDReader.getAncestors(goTerm1, goTermList, new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GOTermImpl("GO1", null)));
    }

    @Test
    void getAncestorsWithoutGoTermAndAncertors() {
        GOTerm goTerm1 = new GOTermImpl("GO1", "TERM1");
        Set<GOTerm> goTermRelations =
                GORelationRDDReader.getAncestors(goTerm1, new ArrayList<>(), new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GOTermImpl("GO1", null)));
    }

    @Test
    void getValidGoTermById() {
        GOTerm goTerm1 = new GOTermImpl("GO1", "TERM1");
        List<GOTerm> goTermList = Collections.singletonList(goTerm1);
        GOTerm result = GORelationRDDReader.getGoTermById("GO1", goTermList);

        assertNotNull(result);
        assertEquals(goTerm1, result);
    }

    @Test
    void getInValidGoTermById() {
        GOTerm goTerm1 = new GOTermImpl("GO1", "TERM1");
        List<GOTerm> goTermList = Collections.singletonList(goTerm1);
        GOTerm result = GORelationRDDReader.getGoTermById("GO2", goTermList);

        assertNotNull(result);
        assertEquals("GO2", result.getId());
        assertNull(result.getName());
    }
}
