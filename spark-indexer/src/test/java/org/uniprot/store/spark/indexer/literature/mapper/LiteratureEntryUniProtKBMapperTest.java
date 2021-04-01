package org.uniprot.store.spark.indexer.literature.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;

import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.*;
import org.uniprot.core.literature.LiteratureEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 31/03/2021
 */
class LiteratureEntryUniProtKBMapperTest {

    @Test
    void mapEntryJournalArticleCitation() throws Exception {
        String input =
                "ID   PRKN_HUMAN              Reviewed;         465 AA.\n"
                        + "RN   [1]\n"
                        + "RP   NUCLEOTIDE SEQUENCE [MRNA] (ISOFORMS 1 AND 2), AND INVOLVEMENT IN\n"
                        + "RP   PARK2.\n"
                        + "RC   TISSUE=Fetal brain, and Skeletal muscle;\n"
                        + "RX   PubMed=9560156; DOI=10.1038/33416;\n"
                        + "RA   Kitada T., Asakawa S., Hattori N., Matsumine H., Yamamura Y.,\n"
                        + "RA   Minoshima S., Yokochi M., Mizuno Y., Shimizu N.;\n"
                        + "RT   \"Mutations in the parkin gene cause autosomal recessive juvenile\n"
                        + "RT   parkinsonism.\";\n"
                        + "RL   Nature 392:605-608(1998).";

        LiteratureEntryUniProtKBMapper mapper = new LiteratureEntryUniProtKBMapper();
        Iterator<Tuple2<String, LiteratureEntry>> result = mapper.call(input);
        assertNotNull(result);

        assertTrue(result.hasNext());
        Tuple2<String, LiteratureEntry> tuple = result.next();
        assertNotNull(tuple._1);
        assertEquals("9560156", tuple._1);

        assertNotNull(tuple._2);
        LiteratureEntry entry = tuple._2;
        assertEquals(CitationType.JOURNAL_ARTICLE, entry.getCitation().getCitationType());
        JournalArticle journalArticle = (JournalArticle) entry.getCitation();
        assertEquals("9560156", journalArticle.getId());
        assertEquals("Nature", journalArticle.getJournal().getName());
        assertEquals(
                "Mutations in the parkin gene cause autosomal recessive juvenile parkinsonism.",
                journalArticle.getTitle());
        assertEquals("392", journalArticle.getVolume());
        assertEquals("605", journalArticle.getFirstPage());
        assertEquals("608", journalArticle.getLastPage());
        assertNotNull(journalArticle.getAuthors());
        assertEquals(9, journalArticle.getAuthors().size());
        assertTrue(
                journalArticle.getCitationCrossReferenceByType(CitationDatabase.DOI).isPresent());

        assertNotNull(entry.getStatistics());
        assertEquals(1L, entry.getStatistics().getReviewedProteinCount());
        assertFalse(result.hasNext());
    }

    @Test
    void mapEntrySubmissionCitation() throws Exception {
        String input =
                "ID   PRKN_HUMAN              Unreviewed;         465 AA.\n"
                        + "RN   [3]\n"
                        + "RP   NUCLEOTIDE SEQUENCE [MRNA] (ISOFORMS 3 AND 4).\n"
                        + "RA   D'Agata V., Scapagnini G., Cavallaro S.;\n"
                        + "RT   \"Functional and molecular diversity of parkin.\";\n"
                        + "RL   Submitted (MAY-2001) to the EMBL/GenBank/DDBJ databases.";

        LiteratureEntryUniProtKBMapper mapper = new LiteratureEntryUniProtKBMapper();
        Iterator<Tuple2<String, LiteratureEntry>> result = mapper.call(input);
        assertNotNull(result);

        assertTrue(result.hasNext());
        Tuple2<String, LiteratureEntry> tuple = result.next();
        assertNotNull(tuple._1);
        assertEquals("CI-26ETECPL88BPB", tuple._1);

        assertNotNull(tuple._2);
        LiteratureEntry entry = tuple._2;
        assertNotNull(entry.getCitation());
        assertEquals(CitationType.SUBMISSION, entry.getCitation().getCitationType());
        Submission submission = (Submission) entry.getCitation();
        assertEquals("CI-26ETECPL88BPB", submission.getId());
        assertEquals(SubmissionDatabase.EMBL_GENBANK_DDBJ, submission.getSubmissionDatabase());
        assertEquals("Functional and molecular diversity of parkin.", submission.getTitle());
        assertTrue(submission.getCitationCrossReferences().isEmpty());
        assertNotNull(submission.getAuthors());
        assertEquals(3, submission.getAuthors().size());

        assertNotNull(entry.getStatistics());
        assertEquals(1L, entry.getStatistics().getUnreviewedProteinCount());
        assertFalse(result.hasNext());
    }
}
