package org.uniprot.store.indexer.literature.reader;

import static org.junit.jupiter.api.Assertions.*;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.impl.AuthorImpl;
import org.uniprot.core.literature.LiteratureEntry;

/**
 * @author lgonzales
 * @since 2019-07-03
 */
class LiteratureLineMapperTest {

    @Test
    void mapLineWithAllAttributes() throws Exception {
        String entryText =
                "RX   PubMed=1;\n"
                        + "RX   DOI=10.1016/0006-2944(75)90147-7;\n"
                        + "RA   Makar A.B., McMartin K.E.;\n"
                        + "RG   DIAbetes Genetics Replication And Meta-analysis (DIAGRAM) Consortium;\n"
                        + "RT   \"Formate assay in body fluids: application in methanol poisoning.\";\n"
                        + "RL   Biochem Med 13:117-126(1975).\n"
                        + "**   Author's list is incomplete\n"
                        + "\n"
                        + "EM   krausej@thalamus.wustl.edu\n"
                        + "IP   24\n"
                        + "\n"
                        + "Above pH 8.5, pepsinogen is converted into a form which cannot be activated\n"
                        + "to pepsin on exposure to low pH. Intermediate exposure to neutral pH,\n"
                        + "however, returns the protein to a form which can be activated.";

        LiteratureLineMapper mapper = new LiteratureLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 10);
        assertTrue(entry.hasCitation());
        Literature literature = (Literature) entry.getCitation();

        assertTrue(literature.hasPubmedId());
        assertEquals(literature.getPubmedId(), 1L);

        assertTrue(literature.hasDoiId());
        assertEquals(literature.getDoiId(), "10.1016/0006-2944(75)90147-7");

        assertTrue(literature.hasAuthoringGroup());
        MatcherAssert.assertThat(
                literature.getAuthoringGroups(),
                Matchers.contains(
                        "DIAbetes Genetics Replication And Meta-analysis (DIAGRAM) Consortium"));

        assertTrue(literature.hasAuthors());
        MatcherAssert.assertThat(
                literature.getAuthors(),
                Matchers.contains(new AuthorImpl("Makar A.B."), new AuthorImpl("McMartin K.E.")));

        assertTrue(literature.hasFirstPage());
        assertEquals(literature.getFirstPage(), "117");

        assertTrue(literature.hasJournal());
        assertEquals(literature.getJournal().getName(), "Biochem Med");

        assertTrue(literature.hasLastPage());
        assertEquals(literature.getLastPage(), "126");

        assertTrue(literature.hasLiteratureAbstract());
        assertEquals(
                literature.getLiteratureAbstract(),
                "Above pH 8.5, pepsinogen is converted into a form which"
                        + " cannot be activated to pepsin on exposure to low pH. Intermediate exposure to neutral pH, "
                        + "however, returns the protein to a form which can be activated.");

        assertTrue(literature.hasPublicationDate());
        assertEquals(literature.getPublicationDate().getValue(), "1975");

        assertTrue(literature.hasTitle());
        assertEquals(
                literature.getTitle(),
                "Formate assay in body fluids: application in methanol poisoning.");

        assertTrue(literature.hasVolume());
        assertEquals(literature.getVolume(), "13");

        assertFalse(entry.hasStatistics());
        assertFalse(literature.isCompleteAuthorList());
    }

    @Test
    void mapLineWithRequiredOnlyAttributes() throws Exception {
        String entryText =
                "RX   PubMed=1;\n"
                        + "RA   Makar A.B.;\n"
                        + "RL   Biochem Med 13:117(1975).\n"
                        + "\n"
                        + "IP   24\n"
                        + "\n"
                        + "NO ABSTRACT AVAILABLE";

        LiteratureLineMapper mapper = new LiteratureLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 10);

        assertTrue(entry.hasCitation());
        Literature literature = (Literature) entry.getCitation();

        assertTrue(literature.hasPubmedId());
        assertEquals(literature.getPubmedId(), 1L);

        assertTrue(literature.hasAuthors());
        MatcherAssert.assertThat(
                literature.getAuthors(), Matchers.contains(new AuthorImpl("Makar A.B.")));

        assertTrue(literature.hasFirstPage());
        assertEquals(literature.getFirstPage(), "117");

        assertTrue(literature.hasLastPage());
        assertEquals(literature.getLastPage(), "117");

        assertTrue(literature.hasJournal());
        assertEquals(literature.getJournal().getName(), "Biochem Med");

        assertTrue(literature.hasVolume());
        assertEquals(literature.getVolume(), "13");

        assertTrue(literature.hasPublicationDate());
        assertEquals(literature.getPublicationDate().getValue(), "1975");

        assertFalse(literature.hasTitle());
        assertFalse(literature.hasDoiId());
        assertFalse(literature.hasAuthoringGroup());
        assertFalse(literature.hasLiteratureAbstract());
        assertFalse(entry.hasStatistics());
        assertTrue(literature.isCompleteAuthorList());
    }
}
