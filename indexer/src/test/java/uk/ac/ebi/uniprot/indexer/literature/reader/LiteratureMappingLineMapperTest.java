package uk.ac.ebi.uniprot.indexer.literature.reader;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.uniprot.domain.literature.LiteratureEntry;
import uk.ac.ebi.uniprot.domain.literature.LiteratureMappedReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 */
class LiteratureMappingLineMapperTest {

    @Test
    void mapLineWithAnnotationAndCategory() throws Exception {
        String entryText = "X5FSX0\tGAD\t1358782\t126289\t[Pathology & Biotech]Not Associated with PSYCH: schizophrenia.";
        LiteratureMappingLineMapper mapper = new LiteratureMappingLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 1);

        assertTrue(entry.hasPubmedId());
        assertEquals(entry.getPubmedId(), "1358782");

        assertTrue(entry.hasLiteratureMappedReferences());
        assertEquals(entry.getLiteratureMappedReferences().size(), 1);
        LiteratureMappedReference reference = entry.getLiteratureMappedReferences().get(0);

        assertTrue(reference.hasUniprotAccession());
        assertEquals(reference.getUniprotAccession().getValue(), "X5FSX0");

        assertTrue(reference.hasSource());
        assertEquals(reference.getSource(), "GAD");

        assertTrue(reference.hasSourceId());
        assertEquals(reference.getSourceId(), "126289");

        assertTrue(reference.hasAnnotation());
        assertEquals(reference.getAnnotation(), "Not Associated with PSYCH: schizophrenia.");

        assertTrue(reference.hasSourceCategory());
        assertEquals(reference.getSourceCategory().size(), 1);
        MatcherAssert.assertThat(reference.getSourceCategory(), Matchers.contains("Pathology & Biotech"));

    }

    @Test
    void mapLineWithAnnotationOnly() throws Exception {
        String entryText = "X5FSX0\tGAD\t1358782\t126289\tNot Associated with PSYCH: schizophrenia.";
        LiteratureMappingLineMapper mapper = new LiteratureMappingLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 1);

        assertTrue(entry.hasPubmedId());
        assertEquals(entry.getPubmedId(), "1358782");

        assertTrue(entry.hasLiteratureMappedReferences());
        assertEquals(entry.getLiteratureMappedReferences().size(), 1);
        LiteratureMappedReference reference = entry.getLiteratureMappedReferences().get(0);

        assertTrue(reference.hasUniprotAccession());
        assertEquals(reference.getUniprotAccession().getValue(), "X5FSX0");

        assertTrue(reference.hasSource());
        assertEquals(reference.getSource(), "GAD");

        assertTrue(reference.hasSourceId());
        assertEquals(reference.getSourceId(), "126289");

        assertTrue(reference.hasAnnotation());
        assertEquals(reference.getAnnotation(), "Not Associated with PSYCH: schizophrenia.");

        assertFalse(reference.hasSourceCategory());
    }

    @Test
    void mapLineWithCategoryOnly() throws Exception {
        String entryText = "X5FSX0\tGAD\t1358782\t126289\t[Expression][Sequences]";
        LiteratureMappingLineMapper mapper = new LiteratureMappingLineMapper();
        LiteratureEntry entry = mapper.mapLine(entryText, 1);

        assertTrue(entry.hasPubmedId());
        assertEquals(entry.getPubmedId(), "1358782");

        assertTrue(entry.hasLiteratureMappedReferences());
        assertEquals(entry.getLiteratureMappedReferences().size(), 1);
        LiteratureMappedReference reference = entry.getLiteratureMappedReferences().get(0);

        assertTrue(reference.hasUniprotAccession());
        assertEquals(reference.getUniprotAccession().getValue(), "X5FSX0");

        assertTrue(reference.hasSource());
        assertEquals(reference.getSource(), "GAD");

        assertTrue(reference.hasSourceId());
        assertEquals(reference.getSourceId(), "126289");

        assertFalse(reference.hasAnnotation());

        assertTrue(reference.hasSourceCategory());
        assertEquals(reference.getSourceCategory().size(), 2);
        MatcherAssert.assertThat(reference.getSourceCategory(), Matchers.contains("Expression", "Sequences"));
    }
}