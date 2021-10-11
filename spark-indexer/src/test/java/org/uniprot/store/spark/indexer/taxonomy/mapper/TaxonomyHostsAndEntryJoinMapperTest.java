package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;

import scala.Tuple2;

class TaxonomyHostsAndEntryJoinMapperTest {

    @Test
    void mapWithHost() throws Exception {
        TaxonomyHostsAndEntryJoinMapper mapper = new TaxonomyHostsAndEntryJoinMapper();
        String taxId = "9606";
        Iterable<String> hostIds = List.of("1000", "2000");
        TaxonomyEntry entry = new TaxonomyEntryBuilder().taxonId(9606L).build();
        Tuple2<Iterable<String>, Optional<TaxonomyEntry>> hostTuple =
                new Tuple2<>(hostIds, Optional.of(entry));
        Tuple2<String, Tuple2<Iterable<String>, Optional<TaxonomyEntry>>> tuple =
                new Tuple2<>(taxId, hostTuple);

        Iterator<Tuple2<String, Taxonomy>> result = mapper.call(tuple);
        assertNotNull(result);

        List<Tuple2<String, Taxonomy>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(2, resultList.size());

        assertEquals("1000", resultList.get(0)._1);
        assertEquals(9606L, resultList.get(0)._2.getTaxonId());

        assertEquals("2000", resultList.get(1)._1);
        assertEquals(9606L, resultList.get(0)._2.getTaxonId());
    }

    @Test
    void mapWithWithoutEntry() throws Exception {
        TaxonomyHostsAndEntryJoinMapper mapper = new TaxonomyHostsAndEntryJoinMapper();
        String taxId = "9606";
        Iterable<String> hostIds = List.of("1000", "2000");
        TaxonomyEntry entry = new TaxonomyEntryBuilder().taxonId(9606L).build();
        Tuple2<Iterable<String>, Optional<TaxonomyEntry>> hostTuple =
                new Tuple2<>(hostIds, Optional.empty());
        Tuple2<String, Tuple2<Iterable<String>, Optional<TaxonomyEntry>>> tuple =
                new Tuple2<>(taxId, hostTuple);

        Iterator<Tuple2<String, Taxonomy>> result = mapper.call(tuple);
        assertNotNull(result);

        List<Tuple2<String, Taxonomy>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(0, resultList.size());
    }
}
