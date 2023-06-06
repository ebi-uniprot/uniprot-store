package org.uniprot.store.spark.indexer.chebi.mapper;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChebiEntryRowAggregatorTest {

    @Test
    public void testCallWithNullMap() {
        ChebiEntryRowAggregator aggregator = new ChebiEntryRowAggregator();
        // Test data
        Map<String, Seq<String>> map1 = null;
        // Populate map2 with values
        Map<String, Seq<String>> map2 = getMap();
        Map<String, Seq<String>> expected = getMap();
        // Perform aggregation
        Map<String, Seq<String>> result = null;
        try {
            result = aggregator.call(map1, map2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Assert the result
        assertEquals(expected, result);
    }

    @Test
    public void testCallWithEmptyMap() {
        ChebiEntryRowAggregator aggregator = new ChebiEntryRowAggregator();
        // Test data
        Map<String, Seq<String>> map1 = new HashMap<>();
        // Populate map2 with values
        Map<String, Seq<String>> map2 = getMap();
        Map<String, Seq<String>> expected = getMap();
        // Perform aggregation
        Map<String, Seq<String>> result = null;
        try {
            result = aggregator.call(map1, map2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // Assert the result
        assertEquals(expected, result);
    }

    @NotNull
    private static Map<String, Seq<String>> getMap() {
        // Expected result after aggregation
        Map<String, Seq<String>> expected = new HashMap<>();
        expected.put("name", JavaConverters.asScalaIteratorConverter(Arrays.asList("2-hydroxybehenoyl-CoA").iterator()).asScala().toSeq());
        expected.put("rdfs:label", JavaConverters.asScalaIteratorConverter(Arrays.asList("2-hydroxybehenoyl-coenzyme A").iterator()).asScala().toSeq());
        expected.put("owl:onProperty", JavaConverters.asScalaIteratorConverter(Arrays.asList("http://purl.obolibrary.org/obo/chebi#has_major_microspecies_at_pH_7_3").iterator()).asScala().toSeq());
        expected.put("owl:someValuesFrom", JavaConverters.asScalaIteratorConverter(Arrays.asList("http://purl.obolibrary.org/obo/CHEBI_74117").iterator()).asScala().toSeq());
        return expected;
    }
}
