package uk.ac.ebi.uniprot.indexer.crossref;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrossRefReaderTest {

    private static final String DBREF_FTP = "ftp://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/dbxref.txt";
    private static CrossRefReader READER;

    @BeforeAll
    static void setReader() throws IOException {
        READER = new CrossRefReader(DBREF_FTP);
    }
    @Test
    void testReadFile() {
        CrossRefDocument dbxRef = READER.read();
        assertNotNull(dbxRef, "Unable to read the dbxref file");
        verifyDBXRef(dbxRef);
        int count = 1;
        while(READER.read() != null){
            count++;
        }

        assertTrue(count >= 171, "The count doesn't match");
    }

    private void verifyDBXRef(CrossRefDocument dbxRef) {
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