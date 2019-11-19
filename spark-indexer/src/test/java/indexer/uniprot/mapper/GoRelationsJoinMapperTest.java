package indexer.uniprot.mapper;

import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class GoRelationsJoinMapperTest {

    @Test
    void testValidEntryWithGoCrossReferences() throws Exception{
        GoRelationsJoinMapper mapper = new GoRelationsJoinMapper();
        String flatFile = "uniprotkb/O60260.txt";
        List<String> flatFileLines = Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        Iterator<Tuple2<String, String>> result = mapper.call(String.join("\n", flatFileLines));

        assertNotNull(result);
        assertTrue(result.hasNext());
        int size = 0;
        while(result.hasNext()){
            Tuple2<String, String> tuple = result.next();
            assertTrue(tuple._1.startsWith("GO:"));
            assertEquals("O60260", tuple._2);
            size++;
        }
        assertEquals(144, size);
    }

    @Test
    void testValidEntryWithoutGoCrossReferences() throws Exception{
        GoRelationsJoinMapper mapper = new GoRelationsJoinMapper();
        String flatFile = "uniprotkb/O60260.txt";
        List<String> flatFileLines = Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));

        String entryWithoutGoCrossReference = flatFileLines.stream()
                .filter(line -> !line.startsWith("DR   GO;"))
                .collect(Collectors.joining("\n"));
        Iterator<Tuple2<String, String>> result = mapper.call(entryWithoutGoCrossReference);

        assertNotNull(result);
        assertFalse(result.hasNext());
    }

    @Test
    void testInvalidEntry() {
        GoRelationsJoinMapper mapper = new GoRelationsJoinMapper();

        assertThrows(RuntimeException.class, () -> {
            mapper.call("INVALID ENTRY");
        }, "ParseException");
    }
}