package org.uniprot.store.reader.literature;

import static org.junit.jupiter.api.Assertions.*;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.impl.AuthorBuilder;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
class LiteratureConverterTest {

    @Test
    void mapLineWithAllAttributes() {
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

        LiteratureConverter mapper = new LiteratureConverter();
        Literature literature = mapper.convert(entryText);
        assertNotNull(literature);

        assertTrue(literature.hasPubmedId());
        assertEquals(1L, literature.getPubmedId());
        assertEquals("1", literature.getId());

        assertTrue(literature.hasDoiId());
        assertEquals("10.1016/0006-2944(75)90147-7", literature.getDoiId());

        assertTrue(literature.hasAuthoringGroup());
        MatcherAssert.assertThat(
                literature.getAuthoringGroups(),
                Matchers.contains(
                        "DIAbetes Genetics Replication And Meta-analysis (DIAGRAM) Consortium"));

        assertTrue(literature.hasAuthors());
        MatcherAssert.assertThat(
                literature.getAuthors(),
                Matchers.contains(
                        new AuthorBuilder("Makar A.B.").build(),
                        new AuthorBuilder("McMartin K.E.").build()));

        assertTrue(literature.hasFirstPage());
        assertEquals("117", literature.getFirstPage());

        assertTrue(literature.hasJournal());
        assertEquals("Biochem Med", literature.getJournal().getName());

        assertTrue(literature.hasLastPage());
        assertEquals("126", literature.getLastPage());

        assertTrue(literature.hasLiteratureAbstract());
        assertEquals(
                literature.getLiteratureAbstract(),
                "Above pH 8.5, pepsinogen is converted into a form which"
                        + " cannot be activated to pepsin on exposure to low pH. Intermediate exposure to neutral pH, "
                        + "however, returns the protein to a form which can be activated.");

        assertTrue(literature.hasPublicationDate());
        assertEquals("1975", literature.getPublicationDate().getValue());

        assertTrue(literature.hasTitle());
        assertEquals(
                "Formate assay in body fluids: application in methanol poisoning.",
                literature.getTitle());

        assertTrue(literature.hasVolume());
        assertEquals("13", literature.getVolume());

        assertFalse(literature.isCompleteAuthorList());
    }

    @Test
    void mapLineWithRequiredOnlyAttributes() {
        String entryText =
                "RX   PubMed=1;\n"
                        + "RA   Makar A.B.;\n"
                        + "RL   Biochem Med 13:117(1975).\n"
                        + "\n"
                        + "IP   24\n"
                        + "\n"
                        + "NO ABSTRACT AVAILABLE";

        LiteratureConverter mapper = new LiteratureConverter();
        Literature literature = mapper.convert(entryText);
        assertNotNull(literature);

        assertTrue(literature.hasPubmedId());
        assertEquals(1L, literature.getPubmedId());
        assertEquals("1", literature.getId());

        assertTrue(literature.hasAuthors());
        MatcherAssert.assertThat(
                literature.getAuthors(),
                Matchers.contains(new AuthorBuilder("Makar A.B.").build()));

        assertTrue(literature.hasFirstPage());
        assertEquals("117", literature.getFirstPage());

        assertTrue(literature.hasLastPage());
        assertEquals("117", literature.getLastPage());

        assertTrue(literature.hasJournal());
        assertEquals("Biochem Med", literature.getJournal().getName());

        assertTrue(literature.hasVolume());
        assertEquals("13", literature.getVolume());

        assertTrue(literature.hasPublicationDate());
        assertEquals("1975", literature.getPublicationDate().getValue());

        assertFalse(literature.hasTitle());
        assertFalse(literature.hasDoiId());
        assertFalse(literature.hasAuthoringGroup());
        assertFalse(literature.hasLiteratureAbstract());
        assertTrue(literature.isCompleteAuthorList());
    }
}
