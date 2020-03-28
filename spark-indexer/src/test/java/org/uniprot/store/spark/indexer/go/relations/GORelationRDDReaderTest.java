package org.uniprot.store.spark.indexer.go.relations;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;

/**
 * @author lgonzales
 * @since 2019-11-21
 */
class GORelationRDDReaderTest {

    @Test
    void getAncestorsWithAncestors() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = new ArrayList<>();
        goTermList.add(goTerm1);
        goTermList.add(go("GO2", "TERM2"));
        goTermList.add(go("GO3", "TERM3"));
        goTermList.add(go("GO4", "TERM4"));
        goTermList.add(go("GO5", "TERM5"));
        Map<String, Set<String>> relations = new HashMap<>();
        relations.put("GO1", Collections.singleton("GO3"));

        Set<String> go3Relations = new HashSet<>();
        go3Relations.add("GO4");
        go3Relations.add("GO5");
        go3Relations.add("GO1");
        relations.put("GO3", go3Relations);

        Set<GeneOntologyEntry> goTermRelations =
                GORelationRDDReader.getAncestors(goTerm1, goTermList, relations);
        assertNotNull(goTermRelations);
        assertEquals(4, goTermRelations.size());
        assertTrue(goTermRelations.contains(go("GO1", null)));
        assertTrue(goTermRelations.contains(go("GO3", null)));
        assertTrue(goTermRelations.contains(go("GO4", null)));
        assertTrue(goTermRelations.contains(go("GO5", null)));
    }

    @Test
    void getAncestorsWithoutAncestors() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = Collections.singletonList(goTerm1);
        Set<GeneOntologyEntry> goTermRelations =
                GORelationRDDReader.getAncestors(goTerm1, goTermList, new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(go("GO1", null)));
    }

    @Test
    void getAncestorsWithoutGoTermAndAncertors() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        Set<GeneOntologyEntry> goTermRelations =
                GORelationRDDReader.getAncestors(goTerm1, new ArrayList<>(), new HashMap<>());
        assertNotNull(goTermRelations);
        assertEquals(1, goTermRelations.size());
        assertTrue(goTermRelations.contains(go("GO1", null)));
    }

    @Test
    void getValidGoTermById() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = Collections.singletonList(goTerm1);
        GeneOntologyEntry result = GORelationRDDReader.getGoTermById("GO1", goTermList);

        assertNotNull(result);
        assertEquals(goTerm1, result);
    }

    @Test
    void getInValidGoTermById() {
        GeneOntologyEntry goTerm1 = go("GO1", "TERM1");
        List<GeneOntologyEntry> goTermList = Collections.singletonList(goTerm1);
        GeneOntologyEntry result = GORelationRDDReader.getGoTermById("GO2", goTermList);

        assertNotNull(result);
        assertEquals("GO2", result.getId());
        assertNull(result.getName());
    }

    private GeneOntologyEntry go(String id, String name) {
        return new GeneOntologyEntryBuilder().id(id).name(name).build();
    }
}
