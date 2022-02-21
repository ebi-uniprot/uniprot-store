package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;

class ChebiRelatedIdsJoinMapperTest {

    @Test
    void mapRelatedEntriesReturnChebiWithCompleteRelatedIds() throws Exception {
        ChebiRelatedIdsJoinMapper mapper = new ChebiRelatedIdsJoinMapper();

        List<ChebiEntry> completeRelatedIds = new ArrayList<>();
        completeRelatedIds.add(
                new ChebiEntryBuilder().id("444").name("related-444").inchiKey("inch-444").build());

        completeRelatedIds.add(
                new ChebiEntryBuilder().id("555").name("related-555").inchiKey("inch-555").build());

        Long chebiId = 2222L;
        ChebiEntry entry =
                new ChebiEntryBuilder()
                        .id(String.valueOf(chebiId))
                        .name("entryName")
                        .inchiKey("InchValue")
                        .relatedIdsAdd(new ChebiEntryBuilder().id("444").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("555").build())
                        .build();
        Tuple2<ChebiEntry, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(entry, Optional.of(completeRelatedIds));
        Tuple2<Object, ChebiEntry> result = mapper.call(tuple);
        assertNotNull(result);
    }

    @Test
    void mapWithEmptyRelatedIdsReturnSameEntry() throws Exception {
        ChebiRelatedIdsJoinMapper mapper = new ChebiRelatedIdsJoinMapper();

        Long chebiId = 2222L;
        ChebiEntry entry =
                new ChebiEntryBuilder()
                        .id(String.valueOf(chebiId))
                        .name("entryName")
                        .inchiKey("InchValue")
                        .relatedIdsAdd(new ChebiEntryBuilder().id("444").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("555").build())
                        .build();
        Tuple2<ChebiEntry, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(entry, Optional.empty());
        Tuple2<Object, ChebiEntry> result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(chebiId, result._1);
        assertEquals(entry, result._2);
    }
}
