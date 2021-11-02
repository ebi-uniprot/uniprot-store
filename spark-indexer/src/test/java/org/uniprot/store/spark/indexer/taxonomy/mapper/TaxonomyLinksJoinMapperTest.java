package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;

import scala.Tuple2;

class TaxonomyLinksJoinMapperTest {

    @Test
    void mapsLinks() throws Exception {
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        TaxonomyEntry linkEntry1 = new TaxonomyEntryBuilder().linksAdd("linkEntry1").build();
        TaxonomyEntry linkEntry2 = new TaxonomyEntryBuilder().linksAdd("linkEntry2").build();

        Iterable<TaxonomyEntry> links = java.util.List.of(linkEntry1, linkEntry2);
        Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(entry, Optional.of(links));

        TaxonomyLinksJoinMapper mapper = new TaxonomyLinksJoinMapper();
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getLinks());
        assertEquals(2, result.getLinks().size());
        assertTrue(result.getLinks().contains("linkEntry1"));
        assertTrue(result.getLinks().contains("linkEntry2"));
    }

    @Test
    void mapEmpty() throws Exception {
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(entry, Optional.empty());

        TaxonomyLinksJoinMapper mapper = new TaxonomyLinksJoinMapper();
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getLinks());
        assertEquals(0, result.getLinks().size());
    }
}
