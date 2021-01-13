package org.uniprot.store.spark.indexer.uniprot.mapper;

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

        Tuple2<UniProtDocument, Optional<Iterable<Tuple2<String, String>>>> tuple =
                new Tuple2<>(doc, Optional.empty());
        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertNotNull(result.computationalPubmedIds);
        assertEquals(0, result.computationalPubmedIds.size());
    }

    @Test
    void testEntryWithCitations() throws Exception {
        LiteratureMappedToUniProtDocument mapper = new LiteratureMappedToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        List<Tuple2<String, String>> citations = new ArrayList<>();
        citations.add(new Tuple2<>("type1", "123"));
        citations.add(new Tuple2<>("ORCID", "678"));
        citations.add(new Tuple2<>("type2", "456"));
        citations.add(new Tuple2<>("ORCID", "123"));

        Tuple2<UniProtDocument, Optional<Iterable<Tuple2<String, String>>>> tuple =
                new Tuple2<>(doc, Optional.of(citations));
        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertNotNull(result.computationalPubmedIds);
        assertEquals(2, result.computationalPubmedIds.size());
        assertEquals("123", result.computationalPubmedIds.get(0));
        assertEquals("456", result.computationalPubmedIds.get(1));
        assertNotNull(result.communityPubmedIds);
        assertEquals(2, result.communityPubmedIds.size());
        assertEquals("678", result.communityPubmedIds.get(0));
        assertEquals("123", result.communityPubmedIds.get(1));
    }
}
