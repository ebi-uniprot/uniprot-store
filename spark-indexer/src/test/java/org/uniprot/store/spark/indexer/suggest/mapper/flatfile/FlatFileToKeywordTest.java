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
class FlatFileToKeywordTest {

    @Test
    void testFlatFileTokeywordWithkeyword() throws Exception {
        FlatFileToKeyword mapper = new FlatFileToKeyword();

        String input = "AC   Q9BRS2; B2RB28; Q8NDC8; Q96NV9;\n" +
                "KW   3D-structure; ATP-binding; Cytoplasm; Direct protein sequencing; Hydrolase;\n" +
                "KW   Kinase; Magnesium; Metal-binding; Nucleotide-binding; Phosphoprotein;\n" +
                "KW   Polymorphism; Reference proteome; Ribosome biogenesis;\n" +
                "KW   Serine/threonine-protein kinase; Transferase.";
        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        List<Tuple2<String, String>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(15, resultList.size());

        Tuple2<String, String> result = resultList.get(0);
        assertEquals("3d-structure", result._1);
        assertEquals("3D-structure", result._2);

        result = resultList.get(14);
        assertEquals("transferase", result._1);
        assertEquals("Transferase", result._2);
    }

    @Test
    void testFlatFileTokeywordWithoutkeyword() throws Exception {
        FlatFileToKeyword mapper = new FlatFileToKeyword();

        String input = "AC   P35425;";

        Iterator<Tuple2<String, String>> results = mapper.call(input);
        assertNotNull(results);
        assertFalse(results.hasNext());
    }

}
