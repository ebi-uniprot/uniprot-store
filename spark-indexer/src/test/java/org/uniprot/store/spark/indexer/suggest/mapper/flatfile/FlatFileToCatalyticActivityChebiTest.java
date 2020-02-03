package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class FlatFileToCatalyticActivityChebiTest {

    @Test
    void testFlatFileToCatalyticActivityChebiWithoutComment() throws Exception {
        FlatFileToCatalyticActivityChebi mapper = new FlatFileToCatalyticActivityChebi();

        String input = "AC   P35425;\n";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToCatalyticActivityChebiWithoutChebi() throws Exception {
        FlatFileToCatalyticActivityChebi mapper = new FlatFileToCatalyticActivityChebi();

        String input = "AC   Q8YW84;\n" +
                "CC   -!- CATALYTIC ACTIVITY:\n" +
                "CC       Reaction=a plastoquinone + (n+1) H(+)(in) + NADH = a plastoquinol + n\n" +
                "CC         H(+)(out) + NAD(+); Xref=Rhea:RHEA:42608, Rhea:RHEA-COMP:9561,\n" +
                "CC         Rhea:RHEA-COMP:9562;\n" +
                "CC         Evidence={ECO:0000255|HAMAP-Rule:MF_01352};";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToCatalyticActivityChebiWithChebi() throws Exception {
        FlatFileToCatalyticActivityChebi mapper = new FlatFileToCatalyticActivityChebi();

        String input = "AC   Q8YW84;\n" +
                "CC   -!- CATALYTIC ACTIVITY:\n" +
                "CC       Reaction=a plastoquinone + (n+1) H(+)(in) + NADH = a plastoquinol + n\n" +
                "CC         H(+)(out) + NAD(+); Xref=Rhea:RHEA:42608, Rhea:RHEA-COMP:9561,\n" +
                "CC         Rhea:RHEA-COMP:9562, ChEBI:CHEBI:15378, ChEBI:CHEBI:17757,\n" +
                "CC         ChEBI:CHEBI:57540, ChEBI:CHEBI:57945, ChEBI:CHEBI:62192;\n" +
                "CC         Evidence={ECO:0000255|HAMAP-Rule:MF_01352};";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(5, resultList.size());
        Tuple2<String, String> result = resultList.get(0);
        assertEquals("15378", result._1);
        assertEquals("CHEBI:15378", result._2);

        result = resultList.get(4);
        assertEquals("62192", result._1);
        assertEquals("CHEBI:62192", result._2);
    }
}
