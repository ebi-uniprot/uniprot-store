package indexer.literature;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.literature.LiteratureMappedReference;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-12-02
 */
class LiteratureMappedFileMapperTest {

    @Test
    void mapLineWithAnnotationAndCategory() throws Exception {
        String entryText =
                "X5FSX0\tGAD\t1358782\t126289\t[Pathology & Biotech]Not Associated with PSYCH: schizophrenia.";
        LiteratureMappedFileMapper mapper = new LiteratureMappedFileMapper();
        Tuple2<String, LiteratureMappedReference> tuple = mapper.call(entryText);

        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertEquals(tuple._1, "X5FSX0");

        assertNotNull(tuple._2);
        LiteratureMappedReference reference = tuple._2;
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
        assertEquals(reference.getSourceCategory().get(0), "Pathology & Biotech");
    }

    @Test
    void mapLineWithAnnotationOnly() throws Exception {
        String entryText =
                "X5FSX0\tGAD\t1358782\t126289\tNot Associated with PSYCH: schizophrenia.";
        LiteratureMappedFileMapper mapper = new LiteratureMappedFileMapper();
        Tuple2<String, LiteratureMappedReference> tuple = mapper.call(entryText);

        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertEquals(tuple._1, "X5FSX0");

        assertNotNull(tuple._2);
        LiteratureMappedReference reference = tuple._2;

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
        LiteratureMappedFileMapper mapper = new LiteratureMappedFileMapper();
        Tuple2<String, LiteratureMappedReference> tuple = mapper.call(entryText);

        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertEquals(tuple._1, "X5FSX0");

        assertNotNull(tuple._2);
        LiteratureMappedReference reference = tuple._2;

        assertTrue(reference.hasUniprotAccession());
        assertEquals(reference.getUniprotAccession().getValue(), "X5FSX0");

        assertTrue(reference.hasSource());
        assertEquals(reference.getSource(), "GAD");

        assertTrue(reference.hasSourceId());
        assertEquals(reference.getSourceId(), "126289");

        assertFalse(reference.hasAnnotation());

        assertTrue(reference.hasSourceCategory());
        assertEquals(reference.getSourceCategory().size(), 2);
        assertEquals(reference.getSourceCategory().get(0), "Expression");
        assertEquals(reference.getSourceCategory().get(1), "Sequences");
    }
}
