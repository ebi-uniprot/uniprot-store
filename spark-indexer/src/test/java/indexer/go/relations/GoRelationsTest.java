package indexer.go.relations;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-10-28
 */
class GoRelationsTest {

    @Test
    void getAncestorsWithAncestors() {
        GoRelations goRelations = new GoRelations();
        List<GoTerm> goTermList = new ArrayList<>();
        goTermList.add(new GoTermFileReader.GoTermImpl("GO1", "TERM1"));
        goTermList.add(new GoTermFileReader.GoTermImpl("GO2", "TERM2"));
        goTermList.add(new GoTermFileReader.GoTermImpl("GO3", "TERM3"));
        goTermList.add(new GoTermFileReader.GoTermImpl("GO4", "TERM4"));
        goTermList.add(new GoTermFileReader.GoTermImpl("GO5", "TERM5"));
        Map<String, Set<String>> relations = new HashMap<>();
        relations.put("GO1", Collections.singleton("GO3"));

        Set<String> go3Relations = new HashSet<>();
        go3Relations.add("GO4");
        go3Relations.add("GO5");
        go3Relations.add("GO1");
        relations.put("GO3", go3Relations);
        goRelations.addTerms(goTermList);
        goRelations.addRelations(relations);

        Set<GoTerm> goTermRelations = goRelations.getAncestors("GO1");
        assertNotNull(goTermRelations);
        assertEquals(4, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GoTermFileReader.GoTermImpl("GO1", null)));
        assertTrue(goTermRelations.contains(new GoTermFileReader.GoTermImpl("GO3", null)));
        assertTrue(goTermRelations.contains(new GoTermFileReader.GoTermImpl("GO4", null)));
        assertTrue(goTermRelations.contains(new GoTermFileReader.GoTermImpl("GO5", null)));
    }

    @Test
    void getAncestorsWithoutAncestors() {
        GoRelations goRelations = new GoRelations();
        goRelations.addTerms(Collections.singletonList(new GoTermFileReader.GoTermImpl("GO1", "TERM1")));
        Set<GoTerm> goTermRelations = goRelations.getAncestors("GO1");
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GoTermFileReader.GoTermImpl("GO1", null)));
    }

    @Test
    void getAncestorsWithoutGoTermAndAncertors() {
        GoRelations goRelations = new GoRelations();
        Set<GoTerm> goTermRelations = goRelations.getAncestors("GO1");
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(new GoTermFileReader.GoTermImpl("GO1", null)));
    }

}