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
class FlatFileToSubcellularLocationTest {

    @Test
    void testFlatFileToSubcellularLocationWithoutComment() throws Exception {
        FlatFileToSubcellularLocation mapper = new FlatFileToSubcellularLocation();

        String input = "AC   O88174; Q9R0R9;\n";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToSubcellularLocation() throws Exception {
        FlatFileToSubcellularLocation mapper = new FlatFileToSubcellularLocation();

        String input = "AC   Q8YW84;\n" +
                "CC   -!- SUBCELLULAR LOCATION: [Isoform 1]: Cytoplasmic vesicle, secretory\n" +
                "CC       vesicle, acrosome inner membrane; Single-pass type I membrane protein.\n" +
                "CC       Note=Inner acrosomal membrane of spermatozoa.\n" +
                "CC   -!- SUBCELLULAR LOCATION: Cytoplasm, cytoskeleton, flagellum basal body\n" +
                "CC       {ECO:0000269|PubMed:12802074, ECO:0000269|PubMed:9971742}. Cell\n" +
                "CC       projection, cilium, flagellum membrane {ECO:0000305}; Peripheral\n" +
                "CC       membrane protein {ECO:0000305}; Cytoplasmic side. Cytoplasm\n" +
                "CC       {ECO:0000269|PubMed:12802074, ECO:0000269|PubMed:9971742}.";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(7, resultList.size());
        Tuple2<String, String> result = resultList.get(0);
        assertEquals("acrosome inner membrane", result._1);
        assertEquals("acrosome inner membrane", result._2);

        result = resultList.get(6);
        assertEquals("cytoplasm", result._1);
        assertEquals("cytoplasm", result._2);
    }
}
