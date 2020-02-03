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
class FlatFileToOrganismTest {

    @Test
    void testFlatFileToOrganism() throws Exception {
        FlatFileToOrganism mapper = new FlatFileToOrganism();

        String input = "AC   Q66798;\n" +
                "OS   Sudan ebolavirus (strain Maleo-79) (SEBOV) (Sudan Ebola virus).\n" +
                "OC   Viruses; Riboviria; Negarnaviricota; Haploviricotina; Monjiviricetes;\n" +
                "OC   Mononegavirales; Filoviridae; Ebolavirus.\n" +
                "OX   NCBI_TaxID=128949;";
        Tuple2<String, String> result = mapper.call(input);
        assertNotNull(result);
        assertEquals("128949", result._1);
        assertEquals("128949", result._2);
    }

}
