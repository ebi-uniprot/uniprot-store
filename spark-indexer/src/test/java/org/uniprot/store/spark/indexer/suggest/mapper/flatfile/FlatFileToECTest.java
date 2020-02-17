package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class FlatFileToECTest {

    @Test
    void testFlatFileToECWithEC() throws Exception {
        FlatFileToEC mapper = new FlatFileToEC();

        String input =
                "AC   Q9BRS2; B2RB28; Q8NDC8; Q96NV9;\n"
                        + "DE   RecName: Full=Serine/threonine-protein kinase RIO1;\n"
                        + "DE            EC=2.7.11.1 {ECO:0000269|PubMed:22072790};\n"
                        + "DE            EC=3.6.3.-;\n"
                        + "DE   AltName: Full=RIO kinase 1;";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(2, resultList.size());

        Tuple2<String, String> result = resultList.get(0);
        assertEquals("2.7.11.1", result._1);
        assertEquals("2.7.11.1", result._2);

        result = resultList.get(1);
        assertEquals("3.6.3.-", result._1);
        assertEquals("3.6.3.-", result._2);
    }

    @Test
    void testFlatFileToECWithoutEC() throws Exception {
        FlatFileToEC mapper = new FlatFileToEC();

        String input =
                "AC   P35425;\n"
                        + "DE   RecName: Full=Non-structural protein 1 {ECO:0000255|HAMAP-Rule:MF_04088};\n"
                        + "DE            Short=NSP1 {ECO:0000255|HAMAP-Rule:MF_04088};\n"
                        + "DE   AltName: Full=NCVP2 {ECO:0000255|HAMAP-Rule:MF_04088};\n"
                        + "DE   AltName: Full=Non-structural RNA-binding protein 53 {ECO:0000255|HAMAP-Rule:MF_04088};\n"
                        + "DE            Short=NS53 {ECO:0000255|HAMAP-Rule:MF_04088};";

        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }
}
