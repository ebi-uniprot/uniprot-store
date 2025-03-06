package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

class UniProtUniParcSourceJoinTest {

    @Test
    void canMapSources() throws Exception {
        String accession = "P21802";
        Set<String> sources = Set.of("WP_000001");
        Tuple2<String, Optional<Set<String>>> sourcesTuple =
                new Tuple2<>(accession, Optional.of(sources));
        Tuple2<String, Tuple2<String, Optional<Set<String>>>> tuple =
                new Tuple2<>(accession, sourcesTuple);

        UniProtUniParcSourceJoin uniParcSourceJoin = new UniProtUniParcSourceJoin();
        Tuple2<String, Map<String, Set<String>>> result = uniParcSourceJoin.call(tuple);
        assertNotNull(result);
        assertEquals(accession, result._1);
        assertEquals(Map.of(accession, sources), result._2);
    }

    @Test
    void canMapEmptySources() throws Exception {
        String accession = "P21802";
        Tuple2<String, Optional<Set<String>>> sourcesTuple =
                new Tuple2<>(accession, Optional.empty());
        Tuple2<String, Tuple2<String, Optional<Set<String>>>> tuple =
                new Tuple2<>(accession, sourcesTuple);

        UniProtUniParcSourceJoin uniParcSourceJoin = new UniProtUniParcSourceJoin();
        Tuple2<String, Map<String, Set<String>>> result = uniParcSourceJoin.call(tuple);
        assertNotNull(result);
        assertEquals(accession, result._1);
        assertEquals(Map.of(accession, new HashSet<>()), result._2);
    }
}
