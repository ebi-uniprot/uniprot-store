package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;

import scala.Tuple2;

class GoogleUniProtXMLToUniProtEntryMapperTest {

    private GoogleUniProtXMLEntryExtractor extractor;
    private GoogleUniProtXMLToUniProtEntryMapper mapper;

    @BeforeEach
    public void setup() {
        extractor = new GoogleUniProtXMLEntryExtractor();
        mapper = new GoogleUniProtXMLToUniProtEntryMapper();
    }

    @Test
    public void testTwoEntryMappingFromSampleFile() throws Exception {
        Path path = Paths.get("src/test/resources/2020_02/uniprotkb/google-protlm-uniprot.xml");
        assertTrue(Files.exists(path));

        List<String> lines = Files.readAllLines(path);
        List<String> extractedEntries = new ArrayList<>();
        extractor.call(lines.iterator()).forEachRemaining(extractedEntries::add);

        assertEquals(2, extractedEntries.size());

        // Map first entry
        Tuple2<String, UniProtKBEntry> accessionEntry1 = mapper.call(extractedEntries.get(0));
        UniProtKBEntry entry1 = accessionEntry1._2;
        assertNotNull(entry1);
        assertEquals("A0A6A5BR32", entry1.getPrimaryAccession().getValue());

        // Map second entry
        Tuple2<String, UniProtKBEntry> accessionEntry2 = mapper.call(extractedEntries.get(1));
        UniProtKBEntry entry2 = accessionEntry2._2;
        assertNotNull(entry2);
        assertEquals("A0A8C6XQ33", entry2.getPrimaryAccession().getValue());
    }

    @Test
    public void testMalformedXmlThrowsException() {
        String malformedXml = "<entry><name>BrokenEntry</name>";

        RuntimeException thrown =
                assertThrows(RuntimeException.class, () -> mapper.call(malformedXml));

        assertTrue(thrown.getMessage().contains("javax.xml.bind"));
    }

    @Test
    public void testEmptyXmlThrows() {
        String empty = "";
        assertThrows(RuntimeException.class, () -> mapper.call(empty));
    }
}
