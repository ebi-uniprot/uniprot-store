package indexer.uniprot.mapper;

import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class TaxonomyJoinMapperTest {

    @Test
    void testValidEntryWithOrganismOnly() throws Exception {

        TaxonomyJoinMapper mapper = new TaxonomyJoinMapper();

        String flatFile = "uniprotkb/O60260.txt";
        List<String> flatFileLines = Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        Iterator<Tuple2<String, String>> result = mapper.call(String.join("\n", flatFileLines));

        assertNotNull(result);
        assertTrue(result.hasNext());
        int size = 0;
        while (result.hasNext()) {
            Tuple2<String, String> tuple = result.next();
            assertEquals("9606", tuple._1);
            assertEquals("O60260", tuple._2);
            size++;
        }
        assertEquals(1, size);
    }


    @Test
    void testValidEntryWithOrganismAndHost() throws Exception {

        TaxonomyJoinMapper mapper = new TaxonomyJoinMapper();

        String flatFile = "uniprotkb/P03378.txt";
        List<String> flatFileLines = Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        Iterator<Tuple2<String, String>> result = mapper.call(String.join("\n", flatFileLines));

        assertNotNull(result);

        List<Tuple2<String, String>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);

        assertEquals(2, resultList.size());
        assertEquals("11685", resultList.get(0)._1);
        assertEquals("P03378", resultList.get(0)._2);
        assertEquals("9606", resultList.get(1)._1);
        assertEquals("P03378", resultList.get(1)._2);


    }

}