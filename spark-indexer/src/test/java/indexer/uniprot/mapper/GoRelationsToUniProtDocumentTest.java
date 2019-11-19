package indexer.uniprot.mapper;

import indexer.go.relations.GoTerm;
import indexer.go.relations.GoTermImpl;
import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import scala.Tuple2;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class GoRelationsToUniProtDocumentTest {

    @Test
    void testDocumentWithValidGoRelations() throws Exception {
        GoRelationsToUniProtDocument mapper = new GoRelationsToUniProtDocument();

        Set<GoTerm> ancestors = new HashSet<>();
        ancestors.add(new GoTermImpl("GO:0011111", "Ancestor 1"));
        ancestors.add(new GoTermImpl("GO:0022222", "Ancestor 2"));
        List<GoTerm> goTerms = new ArrayList<>();
        goTerms.add(new GoTermImpl("GO:0012345", "Go Term", ancestors));

        List<String> goValues = new ArrayList<>();
        goValues.add("0012345");

        UniProtDocument doc = new UniProtDocument();
        doc.goWithEvidenceMaps.put("ida", goValues);

        Tuple2<UniProtDocument, Optional<Iterable<GoTerm>>> tuple = new Tuple2<>(doc, Optional.of(goTerms));
        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertEquals(4, result.goes.size());
        assertTrue(result.goes.contains("0011111"));
        assertTrue(result.goes.contains("Ancestor 1"));
        assertTrue(result.goes.contains("0022222"));
        assertTrue(result.goes.contains("Ancestor 2"));

        assertEquals(2, result.goIds.size());
        assertTrue(result.goIds.contains("0011111"));
        assertTrue(result.goIds.contains("0022222"));

        Collection<String> mappedGo = result.goWithEvidenceMaps.get("ida");
        assertEquals(5, mappedGo.size());
        assertTrue(mappedGo.contains("0011111"));
        assertTrue(mappedGo.contains("Ancestor 1"));
        assertTrue(mappedGo.contains("0022222"));
        assertTrue(mappedGo.contains("Ancestor 2"));

        assertTrue(result.content.isEmpty());

    }

    @Test
    void testDocumentWithEmptyGoRelations() throws Exception {
        GoRelationsToUniProtDocument mapper = new GoRelationsToUniProtDocument();

        Tuple2<UniProtDocument, Optional<Iterable<GoTerm>>> tuple = new Tuple2<>(new UniProtDocument(), Optional.empty());
        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertTrue(result.goes.isEmpty());
        assertTrue(result.goIds.isEmpty());
    }


    @Test
    void testDocumentWithInvalidGoMapRelations() throws Exception {
        GoRelationsToUniProtDocument mapper = new GoRelationsToUniProtDocument();

        Set<GoTerm> ancestors = new HashSet<>();
        ancestors.add(new GoTermImpl("GO:0011111", "Ancestor 1"));
        ancestors.add(new GoTermImpl("GO:0022222", "Ancestor 2"));
        List<GoTerm> goTerms = new ArrayList<>();
        goTerms.add(new GoTermImpl("GO:0012345", "Go Term", ancestors));

        Tuple2<UniProtDocument, Optional<Iterable<GoTerm>>> tuple = new Tuple2<>(new UniProtDocument(), Optional.of(goTerms));
        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertEquals(4, result.goes.size());
        assertTrue(result.goes.contains("0011111"));
        assertTrue(result.goes.contains("Ancestor 1"));
        assertTrue(result.goes.contains("0022222"));
        assertTrue(result.goes.contains("Ancestor 2"));

        assertEquals(2, result.goIds.size());
        assertTrue(result.goIds.contains("0011111"));
        assertTrue(result.goIds.contains("0022222"));

        assertTrue(result.goWithEvidenceMaps.isEmpty());
        assertTrue(result.content.isEmpty());

    }

}