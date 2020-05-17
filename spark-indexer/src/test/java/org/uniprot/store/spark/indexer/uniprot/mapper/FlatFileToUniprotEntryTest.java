package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.uniprot.converter.SupportingDataMapHDSFImpl;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class FlatFileToUniprotEntryTest {

    @Test
    void testValidEntry() throws Exception {
        String keywordFile = "2020_02/keyword/keywlist.txt";
        String diseaseFile = "2020_02/disease/humdisease.txt";
        String subcellFile = "2020_02/subcell/subcell.txt";
        String flatFile = "2020_02/uniprotkb/Q9EPI6.sp";
        SupportingDataMapHDSFImpl supportingDataMap =
                new SupportingDataMapHDSFImpl(keywordFile, diseaseFile, subcellFile, null);
        FlatFileToUniprotEntry mapper = new FlatFileToUniprotEntry(supportingDataMap);

        List<String> flatFileLines =
                Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        Tuple2<String, UniProtKBEntry> mappedEntry = mapper.call(String.join("\n", flatFileLines));

        assertNotNull(mappedEntry);
        assertNotNull(mappedEntry._1);
        assertEquals("Q9EPI6", mappedEntry._1);
        assertNotNull(mappedEntry._2);
        assertEquals("Q9EPI6", mappedEntry._2.getPrimaryAccession().getValue());
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
