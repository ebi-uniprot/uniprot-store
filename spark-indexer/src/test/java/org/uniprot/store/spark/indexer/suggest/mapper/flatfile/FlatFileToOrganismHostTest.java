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
class FlatFileToOrganismHostTest {

    @Test
    void testFlatFileToOrganismHostWithOrganismHost() throws Exception {
        FlatFileToOrganismHost mapper = new FlatFileToOrganismHost();

        String input = "AC   Q66798;\n" +
                "OS   Sudan ebolavirus (strain Maleo-79) (SEBOV) (Sudan Ebola virus).\n" +
                "OC   Viruses; Riboviria; Negarnaviricota; Haploviricotina; Monjiviricetes;\n" +
                "OC   Mononegavirales; Filoviridae; Ebolavirus.\n" +
                "OX   NCBI_TaxID=128949;\n" +
                "OH   NCBI_TaxID=77231; Epomops franqueti (Franquet's epauleted fruit bat).\n" +
                "OH   NCBI_TaxID=9606; Homo sapiens (Human).\n" +
                "OH   NCBI_TaxID=77243; Myonycteris torquata (Little collared fruit bat).";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(3, resultList.size());

        Tuple2<String, String> result = resultList.get(0);
        assertEquals("77231", result._1);
        assertEquals("77231", result._2);

        result = resultList.get(2);
        assertEquals("77243", result._1);
        assertEquals("77243", result._2);
    }

    @Test
    void testFlatFileToOrganismHostWithoutOrganismHost() throws Exception {
        FlatFileToOrganismHost mapper = new FlatFileToOrganismHost();

        String input = "AC   P35425;";

        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

}
