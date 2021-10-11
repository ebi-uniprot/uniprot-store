package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

class TaxonomyOtherNamesJoinMapperTest {

    @Test
    void mapOtherNames() throws Exception {
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        TaxonomyEntry otherNamesEntry1 = new TaxonomyEntryBuilder()
                .otherNamesAdd("otherNames1")
                .build();
        TaxonomyEntry otherNamesEntry2 = new TaxonomyEntryBuilder()
                .otherNamesAdd("otherNames2")
                .build();

        Iterable<TaxonomyEntry> otherNames = java.util.List.of(otherNamesEntry1, otherNamesEntry2);
        Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>> tuple = new Tuple2<>(entry, Optional.of(otherNames));

        TaxonomyOtherNamesJoinMapper mapper = new TaxonomyOtherNamesJoinMapper();
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getOtherNames());
        assertEquals(2, result.getOtherNames().size());
        assertTrue(result.getOtherNames().contains("otherNames1"));
        assertTrue(result.getOtherNames().contains("otherNames2"));
    }

    @Test
    void mapEmpty() throws Exception {
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>> tuple = new Tuple2<>(entry, Optional.empty());

        TaxonomyOtherNamesJoinMapper mapper = new TaxonomyOtherNamesJoinMapper();
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getOtherNames());
        assertEquals(0, result.getOtherNames().size());
    }

}