package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

class GraphMergeVertexMapperTest {

    @Test
    void canMerge() {
        GraphMergeVertexMapper mapper = new GraphMergeVertexMapper();
        ChebiEntry entry1 =
                new ChebiEntryBuilder()
                        .id("1")
                        .relatedIdsAdd(new ChebiEntryBuilder().id("2").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("3").build())
                        .build();

        ChebiEntry entry2 =
                new ChebiEntryBuilder()
                        .id("1")
                        .relatedIdsAdd(
                                new ChebiEntryBuilder().id("2").name("n2").inchiKey("i2").build())
                        .relatedIdsAdd(
                                new ChebiEntryBuilder()
                                        .id("20")
                                        .name("n20")
                                        .inchiKey("i20")
                                        .build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("30").build())
                        .build();

        ChebiEntry result = mapper.apply(entry1, entry2);
        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals(4, result.getRelatedIds().size());

        List<String> relatedIds =
                result.getRelatedIds().stream().map(ChebiEntry::getId).collect(Collectors.toList());
        assertTrue(relatedIds.contains("2"));
        assertTrue(relatedIds.contains("3"));
        assertTrue(relatedIds.contains("30"));
        assertTrue(relatedIds.contains("20"));

        ChebiEntry related2 =
                result.getRelatedIds().stream()
                        .filter(r1 -> r1.getId().equals("2"))
                        .findFirst()
                        .orElseThrow(AssertionError::new);

        assertEquals("2", related2.getId());
        assertEquals("n2", related2.getName());
        assertEquals("i2", related2.getInchiKey());
    }
}
