package uk.ac.ebi.uniprot.indexer.common.processor;

import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.uniprot.indexer.common.DocumentConversionException;
import uk.ac.ebi.uniprot.indexer.common.model.AbstractEntryDocumentPair;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.search.document.Document;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
class EntryDocumentPairProcessorTest {
    private static final String INDEXING_DOC_CONVERSION_FAILED_ENTRIES_LOG = "indexing-doc-conversion-failed-entries.error";
    private BasicDocumentPairProcessor converter;

    @BeforeEach
    void beforeEach() {
        this.converter = new BasicDocumentPairProcessor(new BasicConverter());
    }

    @Test
    void onConversionErrorWriteFailedEntryToFile() throws Exception {
        // GIVEN --------------------------------
        String logFileNameForErrors = INDEXING_DOC_CONVERSION_FAILED_ENTRIES_LOG;
        Path logFileForErrors = Paths.get(logFileNameForErrors);
        // truncate any previous log file used to store document conversion errors ...
        // so that we can check for new content later
        if (Files.exists(logFileForErrors)) {
            PrintWriter fileWriter = new PrintWriter(logFileNameForErrors);
            fileWriter.print("");
            fileWriter.close();
        }

        BasicEntry entry = new BasicEntry("entry");
        String entryContents = converter.entryToString(entry);
        BasicEntryDocumentPair pair = new BasicEntryDocumentPair(entry);

        // WHEN --------------------------------
        // ensure an exception is thrown when being processed
        converter.process(pair);

        // wait for the file to be written
        Thread.sleep(500);

        // THEN --------------------------------
        // ensure this entry is written to the error log
        assertThat(Files.exists(logFileForErrors), is(true));

        // sanity check: ensure the error log contains the correct accession
        List<String> lines = Files.lines(logFileForErrors)
                .collect(Collectors.toList());
        assertThat(lines, hasSize(1));
        assertThat(lines, contains(entryContents));
    }

    private static class BasicDocumentPairProcessor extends EntryDocumentPairProcessor<BasicEntry, BasicDocument, BasicEntryDocumentPair> {
        BasicDocumentPairProcessor(DocumentConverter<BasicEntry, BasicDocument> converter) {
            super(converter);
        }

        @Override
        public String extractEntryId(BasicEntry entry) {
            return "id:" + entry.getValue();
        }

        @Override
        public String entryToString(BasicEntry entry) {
            return "ff:" + entry.getValue();
        }
    }

    private static class BasicConverter implements DocumentConverter<BasicEntry, BasicDocument> {
        @Override
        public BasicDocument convert(BasicEntry source) {
            throw new DocumentConversionException("A deliberate mistake");
        }
    }

    private static class BasicEntryDocumentPair extends AbstractEntryDocumentPair<BasicEntry, BasicDocument> {
        BasicEntryDocumentPair(BasicEntry entry) {
            super(entry);
        }
    }

    @Getter
    private static class BasicDocument implements Document {
        private final String value;

        BasicDocument(String value) {
            this.value = value;
        }

        @Override
        public String getDocumentId() {
            return value;
        }
    }

    @Getter
    private static class BasicEntry {
        private final String value;

        BasicEntry(String value) {
            this.value = value;
        }
    }
}