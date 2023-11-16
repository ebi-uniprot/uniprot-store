package org.uniprot.store.indexer.crossref;

import static org.junit.jupiter.api.Assertions.*;

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
        assertEquals("Implicit", dbxRef.getLinkType());
        int count = 1;
        String crossRefWithMultipleServers = "DB-0218";
        boolean serversCrossRefVerified = false;
        while ((dbxRef = reader.read()) != null) {
            count++;
            if (crossRefWithMultipleServers.equals(dbxRef.getId())) {
                verifyCrossRefWithMultipleServers(dbxRef);
                serversCrossRefVerified = true;
            }
        }
        assertEquals(8, count);
        assertTrue(serversCrossRefVerified, "multiple servers cross not found");
    }

    void verifyCrossRefWithMultipleServers(CrossRefEntry dbxRef) {
        assertFalse(dbxRef.getServers().isEmpty());
        assertEquals(2, dbxRef.getServers().size());
        assertEquals("https://www.disgenet.org/", dbxRef.getServers().get(0));
        assertEquals("https://www.disgenetplus.com/", dbxRef.getServers().get(1));
    }

    private void verifyDBXRef(CrossRefEntry dbxRef) {
        System.out.println(dbxRef.getId());
        assertNotNull(dbxRef.getId(), "id is null");
        assertNotNull(dbxRef.getAbbrev(), "Abbrev is null");
        assertNotNull(dbxRef.getName(), "Name is null");
        assertNotNull(dbxRef.getPubMedId(), "PUBMED ID is null");
        assertNotNull(dbxRef.getDoiId(), "DOI Id is null");
        assertNotNull(dbxRef.getLinkType(), "Link Type is null");
        assertNotNull(dbxRef.getServers(), "Server is null");
        assertFalse(dbxRef.getServers().isEmpty());
        assertNotNull(dbxRef.getDbUrl(), "DB URL is null");
        assertNotNull(dbxRef.getCategory(), "Category is null");
    }
}
