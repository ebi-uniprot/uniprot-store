package org.uniprot.store.spark.indexer.genecentric.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.genecentric.impl.ProteinBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 23/10/2020
 */
class GeneCentricJoinTest {

    @Test
    void canJoinCanonicalWithRelated() throws Exception {
        GeneCentricJoin joinMapper = new GeneCentricJoin();
        Protein canonicalProtein = new ProteinBuilder().id("P12345").build();
        GeneCentricEntry canonicalEntry =
                new GeneCentricEntryBuilder()
                        .proteomeId("UP0000005554")
                        .canonicalProtein(canonicalProtein)
                        .build();

        Protein relatedProtein = new ProteinBuilder().id("P54321").build();
        GeneCentricEntry relatedEntry =
                new GeneCentricEntryBuilder()
                        .proteomeId("UP0000005554")
                        .relatedProteinsAdd(relatedProtein)
                        .build();

        Iterable<GeneCentricEntry> related = Collections.singleton(relatedEntry);
        Tuple2<GeneCentricEntry, Optional<Iterable<GeneCentricEntry>>> input =
                new Tuple2<>(canonicalEntry, Optional.of(related));
        GeneCentricEntry result = joinMapper.call(input);

        assertNotNull(result);
        assertNotNull(result.getProteomeId());
        assertEquals("UP0000005554", result.getProteomeId());

        assertNotNull(result.getCanonicalProtein());
        assertEquals(canonicalProtein, result.getCanonicalProtein());

        assertNotNull(result.getRelatedProteins());
        assertFalse(result.getRelatedProteins().isEmpty());
        assertEquals(1, result.getRelatedProteins().size());
        Protein relatedResult = result.getRelatedProteins().get(0);
        assertEquals(relatedProtein, relatedResult);
    }

    @Test
    void canJoinCanonicalWithoutRelatedProteins() throws Exception {
        GeneCentricJoin joinMapper = new GeneCentricJoin();
        Protein canonicalProtein = new ProteinBuilder().id("P12345").build();
        GeneCentricEntry canonicalEntry =
                new GeneCentricEntryBuilder()
                        .proteomeId("UP0000005554")
                        .canonicalProtein(canonicalProtein)
                        .build();

        Tuple2<GeneCentricEntry, Optional<Iterable<GeneCentricEntry>>> input =
                new Tuple2<>(canonicalEntry, Optional.empty());
        GeneCentricEntry result = joinMapper.call(input);

        assertNotNull(result);
        assertNotNull(result.getProteomeId());
        assertEquals("UP0000005554", result.getProteomeId());

        assertNotNull(result.getCanonicalProtein());
        assertEquals(canonicalProtein, result.getCanonicalProtein());

        assertNotNull(result.getRelatedProteins());
        assertTrue(result.getRelatedProteins().isEmpty());
    }
}
