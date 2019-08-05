package org.uniprot.store.indexer.literature.reader;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.impl.AuthorImpl;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.indexer.literature.reader.LiteratureLineMapper;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-07-03
 */
class LiteratureLineMapperTest {

    @Test
    void mapLineWithAllAttributes() throws Exception {
        String entryText = "RX   PubMed=1;\n" +
                "RX   DOI=10.1016/0006-2944(75)90147-7;\n" +
                "RA   Makar A.B., McMartin K.E.;\n" +
                "RG   DIAbetes Genetics Replication And Meta-analysis (DIAGRAM) Consortium;\n" +
                "RT   \"Formate assay in body fluids: application in methanol poisoning.\";\n" +
                "RL   Biochem Med 13:117-126(1975).\n" +
                "**   Author's list is incomplete\n" +
                "\n" +
                "EM   krausej@thalamus.wustl.edu\n" +
                "IP   24\n" +
                "\n" +
                "Above pH 8.5, pepsinogen is converted into a form which cannot be activated\n" +
                "to pepsin on exposure to low pH. Intermediate exposure to neutral pH,\n" +
                "however, returns the protein to a form which can be activated.";

        LiteratureLineMapper mapper = new LiteratureLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 10);

        assertTrue(entry.hasPubmedId());
        assertEquals(entry.getPubmedId(), 1L);

        assertTrue(entry.hasDoiId());
        assertEquals(entry.getDoiId(), "10.1016/0006-2944(75)90147-7");

        assertTrue(entry.hasAuthoringGroup());
        MatcherAssert.assertThat(entry.getAuthoringGroup(), Matchers.contains("DIAbetes Genetics Replication And Meta-analysis (DIAGRAM) Consortium"));

        assertTrue(entry.hasAuthors());
        MatcherAssert.assertThat(entry.getAuthors(), Matchers.contains(new AuthorImpl("Makar A.B."), new AuthorImpl("McMartin K.E.")));

        assertTrue(entry.hasFirstPage());
        assertEquals(entry.getFirstPage(), "117");

        assertTrue(entry.hasJournal());
        assertEquals(entry.getJournal().getName(), "Biochem Med");

        assertTrue(entry.hasLastPage());
        assertEquals(entry.getLastPage(), "126");

        assertTrue(entry.hasLiteratureAbstract());
        assertEquals(entry.getLiteratureAbstract(), "Above pH 8.5, pepsinogen is converted into a form which" +
                " cannot be activated to pepsin on exposure to low pH. Intermediate exposure to neutral pH, " +
                "however, returns the protein to a form which can be activated.");

        assertTrue(entry.hasPublicationDate());
        assertEquals(entry.getPublicationDate().getValue(), "1975");

        assertTrue(entry.hasTitle());
        assertEquals(entry.getTitle(), "Formate assay in body fluids: application in methanol poisoning.");

        assertTrue(entry.hasVolume());
        assertEquals(entry.getVolume(), "13");

        assertFalse(entry.hasStatistics());
        assertFalse(entry.isCompleteAuthorList());
    }

    @Test
    void mapLineWithRequiredOnlyAttributes() throws Exception {
        String entryText = "RX   PubMed=1;\n" +
                "RA   Makar A.B.;\n" +
                "RL   Biochem Med 13:117(1975).\n" +
                "\n" +
                "IP   24\n" +
                "\n" +
                "NO ABSTRACT AVAILABLE";

        LiteratureLineMapper mapper = new LiteratureLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 10);

        assertTrue(entry.hasPubmedId());
        assertEquals(entry.getPubmedId(), 1L);

        assertTrue(entry.hasAuthors());
        MatcherAssert.assertThat(entry.getAuthors(), Matchers.contains(new AuthorImpl("Makar A.B.")));

        assertTrue(entry.hasFirstPage());
        assertEquals(entry.getFirstPage(), "117");

        assertTrue(entry.hasLastPage());
        assertEquals(entry.getLastPage(), "117");

        assertTrue(entry.hasJournal());
        assertEquals(entry.getJournal().getName(), "Biochem Med");

        assertTrue(entry.hasVolume());
        assertEquals(entry.getVolume(), "13");

        assertTrue(entry.hasPublicationDate());
        assertEquals(entry.getPublicationDate().getValue(), "1975");

        assertFalse(entry.hasTitle());
        assertFalse(entry.hasDoiId());
        assertFalse(entry.hasAuthoringGroup());
        assertFalse(entry.hasLiteratureAbstract());
        assertFalse(entry.hasStatistics());
        assertTrue(entry.isCompleteAuthorList());
    }
}