package indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class LiteratureMappedToUniProtDocumentTest {

    @Test
    void testEntryWithoutCitations() throws Exception {
        LiteratureMappedToUniProtDocument mapper = new LiteratureMappedToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();

        Tuple2<UniProtDocument, Optional<Iterable<String>>> tuple =
                new Tuple2<>(doc, Optional.empty());
        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertNotNull(result.mappedCitation);
        assertEquals(0, result.mappedCitation.size());
    }

    @Test
    void testEntryWithCitations() throws Exception {
        LiteratureMappedToUniProtDocument mapper = new LiteratureMappedToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        List<String> citations = new ArrayList<>();
        citations.add("123");
        citations.add("456");

        Tuple2<UniProtDocument, Optional<Iterable<String>>> tuple =
                new Tuple2<>(doc, Optional.of(citations));
        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertNotNull(result.mappedCitation);
        assertEquals(2, result.mappedCitation.size());
        assertEquals("123", result.mappedCitation.get(0));
        assertEquals("456", result.mappedCitation.get(1));
    }
}
