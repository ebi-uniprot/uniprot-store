package org.uniprot.store.indexer.crossref;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.HashMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.uniprot.core.cv.xdb.CrossRefEntry;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.crossref.readers.CrossRefReader;

class CrossRefReaderTest {
    private static final String DBXREF_PATH = "crossref/test-dbxref.txt";
    private static CrossRefReader reader;

    @BeforeAll
    static void setReader() throws IOException {
        JobExecution jobExecution = new JobExecution(1L);
        jobExecution
                .getExecutionContext()
                .put(Constants.CROSS_REF_PROTEIN_COUNT_KEY, new HashMap<>());
        StepExecution stepExecution = new StepExecution("cross-ref-reader", jobExecution);
        reader = new CrossRefReader(DBXREF_PATH);
        reader.getCrossRefProteinCountMap(stepExecution);
    }

    @Test
    void testReadFile() {
        CrossRefEntry dbxRef = reader.read();
        assertNotNull(dbxRef, "Unable to read the dbxref file");
        verifyDBXRef(dbxRef);
        int count = 1;
        while (reader.read() != null) {
            count++;
        }

        assertEquals(count, 5);
    }

    private void verifyDBXRef(CrossRefEntry dbxRef) {
        assertNotNull(dbxRef.getAccession(), "Accession is null");
        assertNotNull(dbxRef.getAbbrev(), "Abbrev is null");
        assertNotNull(dbxRef.getName(), "Name is null");
        assertNotNull(dbxRef.getPubMedId(), "PUBMED ID is null");
        assertNotNull(dbxRef.getDoiId(), "DOI Id is null");
        assertNotNull(dbxRef.getLinkType(), "Link Type is null");
        assertNotNull(dbxRef.getServer(), "Server is null");
        assertNotNull(dbxRef.getDbUrl(), "DB URL is null");
        assertNotNull(dbxRef.getCategory(), "Category is null");
    }
}
