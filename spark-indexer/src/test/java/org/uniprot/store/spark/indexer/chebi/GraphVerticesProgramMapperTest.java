package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

class GraphVerticesProgramMapperTest {

    @Test
    void applyWithNodeThis() {
        ChebiEntry entry = new ChebiEntryBuilder().id("10").build();
        GraphVerticesProgramMapper mapper = new GraphVerticesProgramMapper();
        ChebiEntry result = mapper.apply(10L, null, entry);
        assertNotNull(result);
        assertEquals(entry, result);
    }

    @Test
    void applyWithNodeIn() {
        ChebiEntry entry = new ChebiEntryBuilder().id("10").build();
        GraphVerticesProgramMapper mapper = new GraphVerticesProgramMapper();
        ChebiEntry result = mapper.apply(10L, entry, null);
        assertNotNull(result);
        assertEquals(entry, result);
    }
}
