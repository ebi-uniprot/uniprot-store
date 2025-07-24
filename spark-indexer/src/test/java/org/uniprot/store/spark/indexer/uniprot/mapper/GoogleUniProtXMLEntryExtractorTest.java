package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

class GoogleUniProtXMLEntryExtractorTest {

    private final GoogleUniProtXMLEntryExtractor extractor = new GoogleUniProtXMLEntryExtractor();

    @Test
    public void testSingleEntryExtraction() throws Exception {
        List<String> lines =
                Arrays.asList(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                        "<entry>",
                        "<name>SampleProtein</name>",
                        "</entry>");

        Iterator<String> result = extractor.call(lines.iterator());
        List<String> entries = new ArrayList<>();
        result.forEachRemaining(entries::add);

        assertEquals(1, entries.size());
        assertTrue(entries.get(0).contains("<entry>"));
        assertTrue(entries.get(0).contains("</entry>"));
        assertTrue(entries.get(0).contains("SampleProtein"));
    }

    @Test
    public void testMultipleEntriesExtraction() throws Exception {
        List<String> lines =
                Arrays.asList(
                        "<entry>",
                        "<name>Protein1</name>",
                        "</entry>",
                        "<entry>",
                        "<name>Protein2</name>",
                        "</entry>");

        Iterator<String> result = extractor.call(lines.iterator());
        List<String> entries = new ArrayList<>();
        result.forEachRemaining(entries::add);

        assertEquals(2, entries.size());
        assertTrue(entries.get(0).contains("Protein1"));
        assertTrue(entries.get(1).contains("Protein2"));
    }

    @Test
    public void testNoEntry() throws Exception {
        List<String> lines =
                Arrays.asList("<root>", "<note>This is not an entry</note>", "</root>");

        Iterator<String> result = extractor.call(lines.iterator());
        List<String> entries = new ArrayList<>();
        result.forEachRemaining(entries::add);

        assertTrue(entries.isEmpty());
    }

    @Test
    public void testIncompleteEntry() throws Exception {
        List<String> lines =
                Arrays.asList(
                        "<entry>", "<name>BrokenEntry</name>"
                        // No closing tag
                        );

        Iterator<String> result = extractor.call(lines.iterator());
        List<String> entries = new ArrayList<>();
        result.forEachRemaining(entries::add);

        assertTrue(entries.isEmpty());
    }

    @Test
    public void testSingleLineEntry() throws Exception {
        List<String> lines = Arrays.asList("<entry><name>QuickEntry</name></entry>");

        Iterator<String> result = extractor.call(lines.iterator());
        List<String> entries = new ArrayList<>();
        result.forEachRemaining(entries::add);

        assertEquals(1, entries.size());
        assertTrue(entries.get(0).contains("QuickEntry"));
    }

    @Test
    public void testSampleFileExtraction() throws Exception {
        Path path = Paths.get("src/test/resources/2020_02/uniprotkb/google-protlm-uniprot.xml");
        assertTrue(Files.exists(path));
        List<String> lines = Files.readAllLines(path);
        Iterator<String> result = extractor.call(lines.iterator());
        List<String> entries = new ArrayList<>();
        result.forEachRemaining(entries::add);
        assertFalse(entries.isEmpty());
        assertEquals(2, entries.size());
        assertTrue(entries.get(0).contains("<entry"));
        assertTrue(entries.get(0).contains("</entry>"));
        assertTrue(entries.get(0).contains("A0A6A5BR32"));
        assertTrue(entries.get(1).contains("<entry"));
        assertTrue(entries.get(1).contains("</entry>"));
        assertTrue(entries.get(1).contains("A0A8C6XQ33"));
    }
}
