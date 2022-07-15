package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.junit.jupiter.api.Test;

import scala.Tuple2;

class ChebiJoinMapperTest {

    @Test
    void mapChebiWithCofactorAndCatalyticValues() throws Exception {
        String flatFile = "2020_02/uniprotkb/Q9JJ00.txt";

        List<String> flatFileLines =
                Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        String entryStr = String.join("\n", flatFileLines);
        ChebiJoinMapper mapper = new ChebiJoinMapper();
        Iterator<Tuple2<String, String>> result = mapper.call(entryStr);
        assertNotNull(result);
        Map<String, String> resultMap = new HashMap<>();
        result.forEachRemaining(tuple -> resultMap.put(tuple._1, tuple._2));
        assertEquals(13, resultMap.size());
        resultMap.values().forEach(acc -> assertEquals("Q9JJ00", acc));
        assertTrue(resultMap.containsKey("59789"));
        assertTrue(resultMap.containsKey("57844"));
        assertTrue(resultMap.containsKey("15377"));
        assertTrue(resultMap.containsKey("29105"));
        assertTrue(resultMap.containsKey("29106"));
        assertTrue(resultMap.containsKey("43474"));
        assertTrue(resultMap.containsKey("57643"));
        assertTrue(resultMap.containsKey("18420"));
        assertTrue(resultMap.containsKey("64612"));
        assertTrue(resultMap.containsKey("57262"));
        assertTrue(resultMap.containsKey("29108"));
        assertTrue(resultMap.containsKey("30616"));
        assertTrue(resultMap.containsKey("33019"));
    }

    @Test
    void mapChebiWithoutValues() throws Exception {
        String flatFile = "2020_02/uniprotkb/Q9EPI6-2.sp";

        List<String> flatFileLines =
                Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        String entryStr = String.join("\n", flatFileLines);
        ChebiJoinMapper mapper = new ChebiJoinMapper();
        Iterator<Tuple2<String, String>> result = mapper.call(entryStr);
        assertNotNull(result);
        assertFalse(result.hasNext());
    }
}
