package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

class UniProtIdTrackerJoinMapperTest {

    @Test
    void emptyMappedDoNothing() throws Exception {
        UniProtDocument doc = new UniProtDocument();
        Tuple2<UniProtDocument, Optional<Set<String>>> tuple2 = new Tuple2<>(doc, Optional.empty());

        UniProtIdTrackerJoinMapper mapper = new UniProtIdTrackerJoinMapper();

        UniProtDocument result = mapper.call(tuple2);
        assertNotNull(result);
        assertEquals(doc, doc);
    }

    @Test
    void canMapIdsForReviewedEntry() throws Exception {
        Set<String> mapped = Set.of("UNI_ID1", "UNI_ID2");
        UniProtDocument doc = new UniProtDocument();
        doc.reviewed = true;
        Tuple2<UniProtDocument, Optional<Set<String>>> tuple2 =
                new Tuple2<>(doc, Optional.of(mapped));

        UniProtIdTrackerJoinMapper mapper = new UniProtIdTrackerJoinMapper();

        UniProtDocument result = mapper.call(tuple2);
        assertNotNull(result);
        assertTrue(doc.id.containsAll(mapped));
        assertTrue(doc.idDefault.containsAll(mapped));
    }

    @Test
    void canAppendIdsForReviewedEntry() throws Exception {
        Set<String> mapped = Set.of("UNI_ID2");
        UniProtDocument doc = new UniProtDocument();
        doc.reviewed = true;
        doc.id.add("UNI_ID1");
        doc.idDefault.add("UNI_ID1");
        Tuple2<UniProtDocument, Optional<Set<String>>> tuple2 =
                new Tuple2<>(doc, Optional.of(mapped));

        UniProtIdTrackerJoinMapper mapper = new UniProtIdTrackerJoinMapper();

        UniProtDocument result = mapper.call(tuple2);
        assertNotNull(result);
        assertEquals(List.of("UNI_ID1", "UNI_ID2"), doc.id);
        assertEquals(List.of("UNI_ID1", "UNI_ID2"), doc.idDefault);
    }

    @Test
    void forUnreviewedEntryThrowsError() throws Exception {
        Set<String> mapped = Set.of("UNI_ID1", "UNI_ID2");
        UniProtDocument doc = new UniProtDocument();
        doc.reviewed = false;
        Tuple2<UniProtDocument, Optional<Set<String>>> tuple2 =
                new Tuple2<>(doc, Optional.of(mapped));

        UniProtIdTrackerJoinMapper mapper = new UniProtIdTrackerJoinMapper();

        assertThrows(SparkIndexException.class, () -> mapper.call(tuple2));
    }
}
