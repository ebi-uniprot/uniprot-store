package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import scala.Tuple2;

class FlatFileToCatalyticActivityRheaCompTest {

    @Test
    void testFlatFileToCatalyticActivityRheaCompWithoutComment() throws Exception {
        FlatFileToCatalyticActivityRheaComp mapper = new FlatFileToCatalyticActivityRheaComp();

        String input = "AC   P35425;\n";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToCatalyticActivityRheaCompWithoutRheaComp() throws Exception {
        FlatFileToCatalyticActivityRheaComp mapper = new FlatFileToCatalyticActivityRheaComp();

        String input =
                "AC   Q8YW84;\n"
                        + "CC   -!- CATALYTIC ACTIVITY:\n"
                        + "CC       Reaction=a plastoquinone + (n+1) H(+)(in) + NADH = a plastoquinol + n\n"
                        + "CC         H(+)(out) + NAD(+); Xref=Rhea:RHEA:42608, ChEBI:CHEBI:15378,\n"
                        + "CC         ChEBI:CHEBI:57540, ChEBI:CHEBI:57945, ChEBI:CHEBI:62192;\n"
                        + "CC         Evidence={ECO:0000255|HAMAP-Rule:MF_01352};";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToCatalyticActivityRheaCompWithRheaComp() throws Exception {
        FlatFileToCatalyticActivityRheaComp mapper = new FlatFileToCatalyticActivityRheaComp();

        String input =
                "AC   Q8YW84;\n"
                        + "CC   -!- CATALYTIC ACTIVITY:\n"
                        + "CC       Reaction=a plastoquinone + (n+1) H(+)(in) + NADH = a plastoquinol + n\n"
                        + "CC         H(+)(out) + NAD(+); Xref=Rhea:RHEA:42608, Rhea:RHEA-COMP:9561,\n"
                        + "CC         Rhea:RHEA-COMP:9562, ChEBI:CHEBI:15378, ChEBI:CHEBI:17757,\n"
                        + "CC         ChEBI:CHEBI:57540, ChEBI:CHEBI:57945, ChEBI:CHEBI:62192;\n"
                        + "CC         Evidence={ECO:0000255|HAMAP-Rule:MF_01352};";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(2, resultList.size());
        Tuple2<String, String> result = resultList.get(0);
        assertEquals("RHEA-COMP:9561", result._1);
        assertEquals("RHEA-COMP:9561", result._2);

        result = resultList.get(1);
        assertEquals("RHEA-COMP:9562", result._1);
        assertEquals("RHEA-COMP:9562", result._2);
    }
}
