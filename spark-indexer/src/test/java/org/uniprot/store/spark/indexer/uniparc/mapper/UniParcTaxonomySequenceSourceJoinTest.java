package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.store.spark.indexer.uniparc.model.UniParcTaxonomySequenceSource;

import scala.Tuple2;

class UniParcTaxonomySequenceSourceJoinTest {

    @Test
    void joinEmptyOptionalValues() throws Exception {
        UniParcTaxonomySequenceSourceJoin join = new UniParcTaxonomySequenceSourceJoin();
        Tuple2<Optional<Iterable<TaxonomyEntry>>, Optional<Map<String, Set<String>>>> tuple =
                new Tuple2<>(Optional.empty(), Optional.empty());

        UniParcTaxonomySequenceSource result = join.call(tuple);

        assertNotNull(result);
        assertEquals(List.of(), result.organisms());
        assertEquals(Map.of(), result.sequenceSources());
    }

    @Test
    void joinWithValues() throws Exception {
        UniParcTaxonomySequenceSourceJoin join = new UniParcTaxonomySequenceSourceJoin();
        List<TaxonomyEntry> organisms = List.of(new TaxonomyEntryBuilder().taxonId(9606).build());
        Map<String, Set<String>> sources = Map.of("K1", Set.of("V1"));
        Tuple2<Optional<Iterable<TaxonomyEntry>>, Optional<Map<String, Set<String>>>> tuple =
                new Tuple2<>(Optional.of(organisms), Optional.of(sources));

        UniParcTaxonomySequenceSource result = join.call(tuple);

        assertNotNull(result);
        assertEquals(organisms, result.organisms());
        assertEquals(sources, result.sequenceSources());
    }
}
