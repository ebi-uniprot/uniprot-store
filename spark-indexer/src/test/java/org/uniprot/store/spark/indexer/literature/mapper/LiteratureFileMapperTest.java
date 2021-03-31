package org.uniprot.store.spark.indexer.literature.mapper;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.impl.AuthorBuilder;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 31/03/2021
 */
class LiteratureFileMapperTest {

    @Test
    void canMapLiterature() throws Exception {
        String input =
                "RX   PubMed=10000;\n"
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

        LiteratureFileMapper mapper = new LiteratureFileMapper();
        Tuple2<String, Literature> result = mapper.call(input);
        assertNotNull(result);

        assertNotNull(result._2);
        Literature literature = (Literature) result._2;

        assertTrue(literature.hasPubmedId());
        assertEquals(10000L, literature.getPubmedId());
        assertEquals("10000", literature.getId());

        assertTrue(literature.hasDoiId());
        assertEquals("10.1016/0006-2944(75)90147-7", literature.getDoiId());

        assertTrue(literature.hasAuthors());
        MatcherAssert.assertThat(
                literature.getAuthors(),
                Matchers.contains(
                        new AuthorBuilder("Makar A.B.").build(),
                        new AuthorBuilder("McMartin K.E.").build()));

        assertTrue(literature.hasAuthoringGroup());
        MatcherAssert.assertThat(
                literature.getAuthoringGroups(),
                Matchers.contains(
                        "DIAbetes Genetics Replication And Meta-analysis (DIAGRAM) Consortium"));

        assertTrue(literature.hasJournal());
        assertEquals("Biochem Med", literature.getJournal().getName() );

        assertTrue(literature.hasFirstPage());
        assertEquals("117", literature.getFirstPage());

        assertTrue(literature.hasLastPage());
        assertEquals("126", literature.getLastPage());

        assertTrue(literature.hasLiteratureAbstract());
        assertEquals(
                literature.getLiteratureAbstract(),
                "Above pH 8.5, pepsinogen is converted into a form which"
                        + " cannot be activated to pepsin on exposure to low pH. Intermediate exposure to neutral pH, "
                        + "however, returns the protein to a form which can be activated.");

        assertTrue(literature.hasVolume());
        assertEquals("13", literature.getVolume());

        assertTrue(literature.hasTitle());
        assertEquals(
                "Formate assay in body fluids: application in methanol poisoning.",
                literature.getTitle());

        assertTrue(literature.hasPublicationDate());
        assertEquals("1975", literature.getPublicationDate().getValue());
    }
}