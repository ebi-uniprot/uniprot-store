package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import scala.Tuple2;

class FlatFileToBindingFeatureChebiTest {

    @Test
    void testFlatFileToBindingFeatureChebiWithoutBindingFeature() throws Exception {
        FlatFileToBindingFeatureChebi mapper = new FlatFileToBindingFeatureChebi();

        String input = "AC   P35425;\n";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToBindingFeatureChebiWithoutChebi() throws Exception {
        FlatFileToBindingFeatureChebi mapper = new FlatFileToBindingFeatureChebi();

        String input =
                "AC   Q8YW84;\n"
                        + "FT   BINDING         140\n"
                        + "FT                   /ligand=\"Zn(2+)\"\n"
                        + "FT                   /ligand_note=\"catalytic\"\n"
                        + "FT   BINDING         146\n"
                        + "FT                   /ligand=\"Zn(2+)\"\n";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToBindingFeatureChebiWithChebi() throws Exception {
        FlatFileToBindingFeatureChebi mapper = new FlatFileToBindingFeatureChebi();

        String input =
                "AC   Q8YW84;\n"
                        + "FT   BINDING         140\n"
                        + "FT                   /ligand=\"Zn(2+)\"\n"
                        + "FT                   /ligand_id=\"ChEBI:CHEBI:2400\"\n"
                        + "FT                   /ligand_note=\"catalytic\"\n"
                        + "FT                   /evidence=\"ECO:0000255|HAMAP-Rule:MF_00009\"\n"
                        + "FT   BINDING         146\n"
                        + "FT                   /ligand=\"Zn(2+)\"\n"
                        + "FT                   /ligand_id=\"ChEBI:CHEBI:6600\"\n"
                        + "FT                   /ligand_note=\"catalytic\"\n"
                        + "FT                   /evidence=\"ECO:0000255|HAMAP-Rule:MF_00009\"";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(2, resultList.size());
        Tuple2<String, String> result = resultList.get(0);
        assertEquals("6600", result._1);
        assertEquals("CHEBI:6600", result._2);

        result = resultList.get(1);
        assertEquals("2400", result._1);
        assertEquals("CHEBI:2400", result._2);
    }
}
