package org.uniprot.store.spark.indexer.common.converter;

import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.*;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.spark.indexer.publication.mapper.UniProtKBPublicationToMappedReference;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 31/03/2021
 */
class UniProtKBReferencesConverterTest {


    @Test
    void loadsCorrectlyMultipleReferences() throws Exception {
        UniProtKBReferencesConverter converter =
                new UniProtKBReferencesConverter();
        String flatFile = "2020_02/uniprotkb/O60260.txt";
        List<String> flatFileLines =
                Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        List<UniProtKBReference> result = converter.convert(flatFileLines.toArray(new String[0]));
        assertNotNull(result);
        assertEquals(84, result.size());
        validateJournalArticleReference(result.get(0));

        validateSubmissionReference(result.get(2));
    }

    private void validateJournalArticleReference(UniProtKBReference refJournal) {
        assertNotNull(refJournal.getCitation());
        assertEquals(CitationType.JOURNAL_ARTICLE, refJournal.getCitation().getCitationType());
        JournalArticle journalArticle = (JournalArticle) refJournal.getCitation();
        assertEquals("9560156", journalArticle.getId());
        assertEquals("Mutations in the parkin gene cause autosomal recessive juvenile parkinsonism.", journalArticle.getTitle());
        assertEquals("Nature", journalArticle.getJournal().getName());
        assertEquals("605", journalArticle.getFirstPage());
        assertEquals("608", journalArticle.getLastPage());
        assertEquals("392", journalArticle.getVolume());
        assertNotNull(journalArticle.getAuthors());
        assertEquals(9, journalArticle.getAuthors().size());
        assertTrue(journalArticle.getCitationCrossReferenceByType(CitationDatabase.DOI).isPresent());

        assertEquals(2, refJournal.getReferencePositions().size());
        assertEquals("NUCLEOTIDE SEQUENCE [MRNA] (ISOFORMS 1 AND 2)", refJournal.getReferencePositions().get(0));
        assertEquals("INVOLVEMENT IN PARK2", refJournal.getReferencePositions().get(1));

        assertEquals(2, refJournal.getReferenceComments().size());
        assertEquals("Fetal brain", refJournal.getReferenceComments().get(0).getValue());
        assertEquals("Skeletal muscle", refJournal.getReferenceComments().get(1).getValue());
    }


    private void validateSubmissionReference(UniProtKBReference refSubmission) {
        assertNotNull(refSubmission.getCitation());
        assertEquals(CitationType.SUBMISSION, refSubmission.getCitation().getCitationType());
        Submission submission = (Submission) refSubmission.getCitation();
        assertEquals("CI-26ETECPL88BPB", submission.getId());
        assertEquals("Functional and molecular diversity of parkin.", submission.getTitle());
        assertEquals(SubmissionDatabase.EMBL_GENBANK_DDBJ, submission.getSubmissionDatabase());
        assertNotNull(submission.getAuthors());
        assertEquals(3, submission.getAuthors().size());
        assertTrue(submission.getCitationCrossReferences().isEmpty());

        assertEquals(1, refSubmission.getReferencePositions().size());
        assertEquals("NUCLEOTIDE SEQUENCE [MRNA] (ISOFORMS 3 AND 4)", refSubmission.getReferencePositions().get(0));

        assertTrue(refSubmission.getReferenceComments().isEmpty());
    }
}