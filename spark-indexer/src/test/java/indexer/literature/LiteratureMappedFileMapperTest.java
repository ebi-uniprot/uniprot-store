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
        assertEquals("1358782", tuple._1);

        assertNotNull(tuple._2);
        LiteratureMappedReference reference = tuple._2;
        assertTrue(reference.hasUniprotAccession());
        assertEquals("X5FSX0", reference.getUniprotAccession().getValue());

        assertTrue(reference.hasSource());
        assertEquals("GAD", reference.getSource());

        assertTrue(reference.hasSourceId());
        assertEquals("126289", reference.getSourceId());

        assertTrue(reference.hasAnnotation());
        assertEquals("Not Associated with PSYCH: schizophrenia.", reference.getAnnotation());

        assertTrue(reference.hasSourceCategory());
        assertEquals(1, reference.getSourceCategory().size());
        assertEquals("Pathology & Biotech", reference.getSourceCategory().get(0));
    }

    @Test
    void mapLineWithAnnotationOnly() throws Exception {
        String entryText =
                "X5FSX0\tGAD\t1358782\t126289\tNot Associated with PSYCH: schizophrenia.";
        LiteratureMappedFileMapper mapper = new LiteratureMappedFileMapper();
        Tuple2<String, LiteratureMappedReference> tuple = mapper.call(entryText);

        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertEquals("1358782", tuple._1);

        assertNotNull(tuple._2);
        LiteratureMappedReference reference = tuple._2;

        assertTrue(reference.hasUniprotAccession());
        assertEquals("X5FSX0", reference.getUniprotAccession().getValue());

        assertTrue(reference.hasSource());
        assertEquals("GAD", reference.getSource());

        assertTrue(reference.hasSourceId());
        assertEquals("126289", reference.getSourceId());

        assertTrue(reference.hasAnnotation());
        assertEquals("Not Associated with PSYCH: schizophrenia.", reference.getAnnotation());

        assertFalse(reference.hasSourceCategory());
    }

    @Test
    void mapLineWithCategoryOnly() throws Exception {
        String entryText = "X5FSX0\tGAD\t1358782\t126289\t[Expression][Sequences]";
        LiteratureMappedFileMapper mapper = new LiteratureMappedFileMapper();
        Tuple2<String, LiteratureMappedReference> tuple = mapper.call(entryText);

        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertEquals("1358782", tuple._1);

        assertNotNull(tuple._2);
        LiteratureMappedReference reference = tuple._2;

        assertTrue(reference.hasUniprotAccession());
        assertEquals("X5FSX0", reference.getUniprotAccession().getValue());

        assertTrue(reference.hasSource());
        assertEquals("GAD", reference.getSource());

        assertTrue(reference.hasSourceId());
        assertEquals("126289", reference.getSourceId());

        assertFalse(reference.hasAnnotation());

        assertTrue(reference.hasSourceCategory());
        assertEquals(2, reference.getSourceCategory().size());
        assertEquals("Expression", reference.getSourceCategory().get(0));
        assertEquals("Sequences", reference.getSourceCategory().get(1));
    }
}
