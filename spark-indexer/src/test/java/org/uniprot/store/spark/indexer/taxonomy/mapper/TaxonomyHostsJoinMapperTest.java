package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

import scala.Tuple2;

class TaxonomyHostsJoinMapperTest {

    @Test
    void mapWithHosts() throws Exception {
        TaxonomyHostsJoinMapper mapper = new TaxonomyHostsJoinMapper();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        List<Taxonomy> hosts = new ArrayList<>();
        hosts.add(new TaxonomyBuilder().taxonId(10).scientificName("tax10").build());
        hosts.add(new TaxonomyBuilder().taxonId(20).scientificName("tax20").build());

        Tuple2<TaxonomyEntry, Optional<Iterable<Taxonomy>>> tuple =
                new Tuple2<>(entry, Optional.of(hosts));
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(hosts, result.getHosts());
    }

    @Test
    void mapWithoutHosts() throws Exception {
        TaxonomyHostsJoinMapper mapper = new TaxonomyHostsJoinMapper();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();

        Tuple2<TaxonomyEntry, Optional<Iterable<Taxonomy>>> tuple =
                new Tuple2<>(entry, Optional.empty());
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertTrue(result.getHosts().isEmpty());
    }
}
