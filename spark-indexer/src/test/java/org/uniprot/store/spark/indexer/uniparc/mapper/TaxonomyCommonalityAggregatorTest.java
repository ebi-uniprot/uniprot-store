package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;

import scala.Tuple2;
import scala.Tuple3;

class TaxonomyCommonalityAggregatorTest {

    @Test
    void testCall_withCommonTaxonomies() throws Exception {
        TaxonomyLineage lineage1_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage1_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();
        TaxonomyLineage lineage1_3 =
                new TaxonomyLineageBuilder().scientificName("C").taxonId(3L).build();

        TaxonomyLineage lineage2_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage2_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();
        TaxonomyLineage lineage2_3 =
                new TaxonomyLineageBuilder().scientificName("D").taxonId(4L).build();

        List<List<TaxonomyLineage>> lineageList =
                Arrays.asList(
                        Arrays.asList(lineage1_1, lineage1_2, lineage1_3),
                        Arrays.asList(lineage2_1, lineage2_2, lineage2_3));

        Tuple2<String, List<List<TaxonomyLineage>>> input = new Tuple2<>("UniParcId", lineageList);

        TaxonomyCommonalityAggregator aggregator = new TaxonomyCommonalityAggregator();
        Tuple2<String, List<Tuple3<String, Long, String>>> result = aggregator.call(input);

        List<Tuple3<String, Long, String>> expectedCommonTaxons = new ArrayList<>();
        expectedCommonTaxons.add(new Tuple3<>("A", 2L, "B"));

        assertEquals("UniParcId", result._1);
        assertEquals(expectedCommonTaxons, result._2);
    }

    @Test
    void testCall_withCommonTaxonomiesWithDifferentLength() throws Exception {
        TaxonomyLineage lineage1_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage1_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();
        TaxonomyLineage lineage1_3 =
                new TaxonomyLineageBuilder().scientificName("C").taxonId(3L).build();

        TaxonomyLineage lineage2_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage2_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();
        TaxonomyLineage lineage2_3 =
                new TaxonomyLineageBuilder().scientificName("D").taxonId(4L).build();

        TaxonomyLineage lineage3_1 =
                new TaxonomyLineageBuilder().scientificName("P").taxonId(16L).build();
        TaxonomyLineage lineage3_2 =
                new TaxonomyLineageBuilder().scientificName("Q").taxonId(17L).build();
        TaxonomyLineage lineage3_3 =
                new TaxonomyLineageBuilder().scientificName("R").taxonId(18L).build();

        TaxonomyLineage lineage4_1 =
                new TaxonomyLineageBuilder().scientificName("P").taxonId(16L).build();
        TaxonomyLineage lineage4_2 =
                new TaxonomyLineageBuilder().scientificName("Q").taxonId(17L).build();

        TaxonomyLineage lineage5_1 =
                new TaxonomyLineageBuilder().scientificName("P").taxonId(16L).build();
        TaxonomyLineage lineage5_2 =
                new TaxonomyLineageBuilder().scientificName("Q").taxonId(17L).build();
        TaxonomyLineage lineage5_3 =
                new TaxonomyLineageBuilder().scientificName("R").taxonId(18L).build();

        List<List<TaxonomyLineage>> lineageList =
                Arrays.asList(
                        Arrays.asList(lineage1_1, lineage1_2, lineage1_3),
                        Arrays.asList(lineage2_1, lineage2_2, lineage2_3),
                        Arrays.asList(lineage3_1, lineage3_2, lineage3_3),
                        Arrays.asList(lineage4_1, lineage4_2),
                        Arrays.asList(lineage5_1, lineage5_2, lineage5_3));

        Tuple2<String, List<List<TaxonomyLineage>>> input = new Tuple2<>("UniParcId", lineageList);

        TaxonomyCommonalityAggregator aggregator = new TaxonomyCommonalityAggregator();
        Tuple2<String, List<Tuple3<String, Long, String>>> result = aggregator.call(input);

        List<Tuple3<String, Long, String>> expectedCommonTaxons = new ArrayList<>();
        expectedCommonTaxons.add(new Tuple3<>("P", 17L, "Q"));
        expectedCommonTaxons.add(new Tuple3<>("A", 2L, "B"));

        assertEquals("UniParcId", result._1);
        assertEquals(expectedCommonTaxons, result._2);
    }

    @Test
    void testCall_noCommonTaxonomy() throws Exception {
        TaxonomyLineage lineage1_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage1_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();

        TaxonomyLineage lineage2_1 =
                new TaxonomyLineageBuilder().scientificName("C").taxonId(3L).build();
        TaxonomyLineage lineage2_2 =
                new TaxonomyLineageBuilder().scientificName("D").taxonId(4L).build();

        List<List<TaxonomyLineage>> lineageList =
                Arrays.asList(
                        Arrays.asList(lineage1_1, lineage1_2),
                        Arrays.asList(lineage2_1, lineage2_2));

        Tuple2<String, List<List<TaxonomyLineage>>> input = new Tuple2<>("UniParcId", lineageList);

        TaxonomyCommonalityAggregator aggregator = new TaxonomyCommonalityAggregator();
        Tuple2<String, List<Tuple3<String, Long, String>>> result = aggregator.call(input);

        List<Tuple3<String, Long, String>> expectedCommonTaxons = new ArrayList<>();
        expectedCommonTaxons.add(new Tuple3<>("A", 2L, "B"));
        expectedCommonTaxons.add(new Tuple3<>("C", 4L, "D"));

        assertEquals("UniParcId", result._1);
        assertEquals(expectedCommonTaxons, result._2);
    }

    @Test
    void testCall_emptyLineages() throws Exception {
        List<List<TaxonomyLineage>> lineageList = new ArrayList<>();

        Tuple2<String, List<List<TaxonomyLineage>>> input = new Tuple2<>("UniParcId", lineageList);

        TaxonomyCommonalityAggregator aggregator = new TaxonomyCommonalityAggregator();
        Tuple2<String, List<Tuple3<String, Long, String>>> result = aggregator.call(input);

        assertEquals("UniParcId", result._1);
        assertEquals(new ArrayList<>(), result._2);
    }

    @Test
    void testFindLastCommonTaxonomy_withCommonTaxonomy() {
        TaxonomyLineage lineage1_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage1_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();
        TaxonomyLineage lineage1_3 =
                new TaxonomyLineageBuilder().scientificName("C").taxonId(3L).build();

        TaxonomyLineage lineage2_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage2_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();
        TaxonomyLineage lineage2_3 =
                new TaxonomyLineageBuilder().scientificName("D").taxonId(4L).build();

        List<List<TaxonomyLineage>> lineageList =
                Arrays.asList(
                        Arrays.asList(lineage1_1, lineage1_2, lineage1_3),
                        Arrays.asList(lineage2_1, lineage2_2, lineage2_3));

        TaxonomyCommonalityAggregator aggregator = new TaxonomyCommonalityAggregator();
        Tuple2<Long, String> result = aggregator.findLastCommonTaxonomy(lineageList);
        assertEquals(2L, result._1);
        assertEquals("B", result._2);
    }

    @Test
    void testFindLastCommonTaxonomy_noCommonTaxonomy() {
        TaxonomyLineage lineage1_1 =
                new TaxonomyLineageBuilder().scientificName("A").taxonId(1L).build();
        TaxonomyLineage lineage1_2 =
                new TaxonomyLineageBuilder().scientificName("B").taxonId(2L).build();

        TaxonomyLineage lineage2_1 =
                new TaxonomyLineageBuilder().scientificName("C").taxonId(3L).build();
        TaxonomyLineage lineage2_2 =
                new TaxonomyLineageBuilder().scientificName("D").taxonId(4L).build();

        List<List<TaxonomyLineage>> lineageList =
                Arrays.asList(
                        Arrays.asList(lineage1_1, lineage1_2),
                        Arrays.asList(lineage2_1, lineage2_2));

        TaxonomyCommonalityAggregator aggregator = new TaxonomyCommonalityAggregator();
        Tuple2<Long, String> result = aggregator.findLastCommonTaxonomy(lineageList);

        assertNull(result._1);
        assertNull(result._2);
    }

    @Test
    void testFindLastCommonTaxonomy_emptyLineages() {
        List<List<TaxonomyLineage>> lineageList = new ArrayList<>();

        TaxonomyCommonalityAggregator aggregator = new TaxonomyCommonalityAggregator();
        Tuple2<Long, String> result = aggregator.findLastCommonTaxonomy(lineageList);

        assertNull(result);
    }
}
