package indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.UniProtEntry;

import scala.Tuple2;
import indexer.uniprot.converter.SupportingDataMapHDSFImpl;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class FlatFileToUniprotEntryTest {

    @Test
    void testValidEntry() throws Exception {
        String keywordFile = "keyword/keywlist.txt";
        String diseaseFile = "disease/humdisease.txt";
        String subcellFile = "subcell/subcell.txt";
        String flatFile = "uniprotkb/Q9EPI6.sp";
        SupportingDataMapHDSFImpl supportingDataMap =
                new SupportingDataMapHDSFImpl(keywordFile, diseaseFile, subcellFile, null);
        FlatFileToUniprotEntry mapper = new FlatFileToUniprotEntry(supportingDataMap);

        List<String> flatFileLines =
                Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        Tuple2<String, UniProtEntry> mappedEntry = mapper.call(String.join("\n", flatFileLines));

        assertNotNull(mappedEntry);
        assertNotNull(mappedEntry._1);
        assertEquals(mappedEntry._1, "Q9EPI6");
        assertNotNull(mappedEntry._2);
        assertEquals(mappedEntry._2.getPrimaryAccession().getValue(), "Q9EPI6");
        // Entry converter has its own test, here we just make sure that the mapper is working as
        // expected..
    }

    @Test
    void testInvalidEntry() throws Exception {
        SupportingDataMapHDSFImpl supportingDataMap =
                new SupportingDataMapHDSFImpl("", "", "", null);
        FlatFileToUniprotEntry mapper = new FlatFileToUniprotEntry(supportingDataMap);

        assertThrows(
                RuntimeException.class,
                () -> {
                    mapper.call("INVALID ENTRY");
                },
                "ParseException");
    }
}
