package org.uniprot.store.indexer.literature.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;

/** @author lgonzales */
class LiteratureMappingLineMapperTest {

    @Test
    void mapLineWithAnnotationAndCategory() throws Exception {
        String entryText =
                "X5FSX0\tGAD\t1358782\t126289\t[Pathology & Biotech]Not Associated with PSYCH: schizophrenia.";
        LiteratureMappingLineMapper mapper = new LiteratureMappingLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 1);
        assertNotNull(entry);

        assertTrue(entry.hasCitation());
        Literature literature = (Literature) entry.getCitation();
        assertTrue(literature.hasPubmedId());
        assertEquals(literature.getPubmedId(), 1358782L);
        LiteratureStatistics stats = entry.getStatistics();
        assertNotNull(stats);
        assertEquals(1L, stats.getComputationallyMappedProteinCount());
    }

    @Test
    void mapLineWithAnnotationOnly() throws Exception {
        String entryText =
                "X5FSX0\tGAD\t1358782\t126289\tNot Associated with PSYCH: schizophrenia.";
        LiteratureMappingLineMapper mapper = new LiteratureMappingLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 1);
        assertNotNull(entry);

        assertTrue(entry.hasCitation());
        Literature literature = (Literature) entry.getCitation();
        assertTrue(literature.hasPubmedId());
        assertEquals(literature.getPubmedId(), 1358782L);
    }

    @Test
    void mapLineWithCategoryOnly() throws Exception {
        String entryText = "X5FSX0\tGAD\t1358782\t126289\t[Expression][Sequences]";
        LiteratureMappingLineMapper mapper = new LiteratureMappingLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 1);
        assertNotNull(entry);

        assertTrue(entry.hasCitation());
        Literature literature = (Literature) entry.getCitation();
        assertTrue(literature.hasPubmedId());
        assertEquals(literature.getPubmedId(), 1358782L);
    }

    @Test
    void mapLineWithOrcid() throws Exception {
        String entryText =
                "P51674\tORCID\t27793698\t0000-0003-2163-948X\t[Function][Subcellular location][Pathology & Biotech][Interaction][Sequence]";
        LiteratureMappingLineMapper mapper = new LiteratureMappingLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 1);
        assertNotNull(entry);

        assertTrue(entry.hasCitation());
        Literature literature = (Literature) entry.getCitation();
        assertTrue(literature.hasPubmedId());
        assertEquals(literature.getPubmedId(), 27793698L);
        LiteratureStatistics stats = entry.getStatistics();
        assertNotNull(stats);
        assertEquals(1L, stats.getCommunityMappedProteinCount());
    }
}
