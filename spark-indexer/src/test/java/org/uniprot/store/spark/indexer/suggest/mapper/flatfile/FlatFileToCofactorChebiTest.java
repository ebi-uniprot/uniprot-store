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
class FlatFileToCofactorChebiTest {

    @Test
    void testFlatFileToCofactorChebiWithoutComment() throws Exception {
        FlatFileToCofactorChebi mapper = new FlatFileToCofactorChebi();

        String input = "AC   P35425;\n";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

    @Test
    void testFlatFileToCofactorChebiWithChebi() throws Exception {
        FlatFileToCofactorChebi mapper = new FlatFileToCofactorChebi();

        String input = "AC   B8HUM7;\n" +
                "CC   -!- COFACTOR:\n" +
                "CC       Name=Mg(2+); Xref=ChEBI:CHEBI:18420;\n" +
                "CC         Evidence={ECO:0000255|HAMAP-Rule:MF_00451};";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(1, resultList.size());
        Tuple2<String, String> result = resultList.get(0);
        assertEquals("18420", result._1);
        assertEquals("CHEBI:18420", result._2);
    }

}
