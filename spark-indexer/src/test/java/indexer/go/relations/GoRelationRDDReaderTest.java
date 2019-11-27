package indexer.go.relations;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GoRelationRDDReaderTest {

    @Test
    void getAncestorsWithAncestors() {
        GoTerm goTerm1 = new GoTermImpl("GO1", "TERM1");
        List<GoTerm> goTermList = new ArrayList<>();
        goTermList.add(goTerm1);
        goTermList.add(new GoTermImpl("GO2", "TERM2"));
        goTermList.add(new GoTermImpl("GO3", "TERM3"));
        goTermList.add(new GoTermImpl("GO4", "TERM4"));
        goTermList.add(new GoTermImpl("GO5", "TERM5"));
        Map<String, Set<String>> relations = new HashMap<>();
        relations.put("GO1", Collections.singleton("GO3"));

        Set<String> go3Relations = new HashSet<>();
        go3Relations.add("GO4");
        go3Relations.add("GO5");
        go3Relations.add("GO1");
        relations.put("GO3", go3Relations);

        Set<GoTerm> goTermRelations = GoRelationRDDReader.getAncestors(goTerm1, goTermList, relations);
        assertNotNull(goTermRelations);
        assertEquals(4, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GoTermImpl("GO1", null)));
        assertTrue(goTermRelations.contains(new GoTermImpl("GO3", null)));
        assertTrue(goTermRelations.contains(new GoTermImpl("GO4", null)));
        assertTrue(goTermRelations.contains(new GoTermImpl("GO5", null)));
    }

    @Test
    void getAncestorsWithoutAncestors() {
        GoTerm goTerm1 = new GoTermImpl("GO1", "TERM1");
        List<GoTerm> goTermList = Collections.singletonList(goTerm1);
        Set<GoTerm> goTermRelations = GoRelationRDDReader.getAncestors(goTerm1, goTermList, new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GoTermImpl("GO1", null)));
    }

    @Test
    void getAncestorsWithoutGoTermAndAncertors() {
        GoTerm goTerm1 = new GoTermImpl("GO1", "TERM1");
        Set<GoTerm> goTermRelations = GoRelationRDDReader.getAncestors(goTerm1, new ArrayList<>(), new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GoTermImpl("GO1", null)));
    }

    @Test
    void getValidGoTermById() {
        GoTerm goTerm1 = new GoTermImpl("GO1", "TERM1");
        List<GoTerm> goTermList = Collections.singletonList(goTerm1);
        GoTerm result = GoRelationRDDReader.getGoTermById("GO1", goTermList);

        assertNotNull(result);
        assertEquals(goTerm1, result);
    }

    @Test
    void getInValidGoTermById() {
        GoTerm goTerm1 = new GoTermImpl("GO1", "TERM1");
        List<GoTerm> goTermList = Collections.singletonList(goTerm1);
        GoTerm result = GoRelationRDDReader.getGoTermById("GO2", goTermList);

        assertNotNull(result);
        assertEquals("GO2", result.getId());
        assertNull(result.getName());
    }
}