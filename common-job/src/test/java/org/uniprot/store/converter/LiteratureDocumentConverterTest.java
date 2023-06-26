package org.uniprot.store.converter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.LiteratureBuilder;
import org.uniprot.core.citation.impl.SubmissionBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * @author lgonzales
 * @since 31/03/2021
 */
class LiteratureDocumentConverterTest {

    @Test
    void canConvertLiteratureToDocument() {
        LiteratureDocumentConverter converter = new LiteratureDocumentConverter();

        CrossReference<CitationDatabase> doiXref =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.DOI)
                        .id("doiIdValue")
                        .build();
        Citation citation =
                new LiteratureBuilder()
                        .completeAuthorList(true)
                        .literatureAbstract("abstractValue")
                        .authoringGroupsAdd("authoringGroupValue")
                        .authorsAdd("authorValue")
                        .title("titleValue")
                        .citationCrossReferencesAdd(doiXref)
                        .journalName("journalNameValue")
                        .publicationDate("2021")
                        .build();

        LiteratureStatistics statistics =
                new LiteratureStatisticsBuilder()
                        .reviewedProteinCount(10)
                        .communityMappedProteinCount(10)
                        .computationallyMappedProteinCount(10)
                        .build();
        LiteratureEntry entry =
                new LiteratureEntryBuilder().citation(citation).statistics(statistics).build();
        LiteratureDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("CI-LIT-AMEL3F1FP836", result.getId());
        assertEquals("CI-LIT-AMEL3F1FP836", result.getDocumentId());
        assertEquals("doiIdValue", result.getDoi());

        assertEquals("titleValue", result.getTitle());
        assertTrue(result.getAuthor().contains("authorValue"));
        assertEquals("journalNameValue", result.getJournal());
        assertEquals("2021", result.getPublished());
        assertEquals("abstractValue", result.getLitAbstract());
        assertTrue(result.getAuthorGroups().contains("authoringGroupValue"));
        assertEquals(4, result.getCitationsWith().size());
        assertTrue(result.getCitationsWith().contains("1_uniprotkb"));
        assertTrue(result.getCitationsWith().contains("2_reviewed"));
        assertTrue(result.getCitationsWith().contains("4_computationally"));
        assertTrue(result.getCitationsWith().contains("5_community"));
    }

    @Test
    void canConvertSubmissionToDocument() {
        LiteratureDocumentConverter converter = new LiteratureDocumentConverter();

        CrossReference<CitationDatabase> doiXref =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.DOI)
                        .id("doiId")
                        .build();
        Citation citation =
                new SubmissionBuilder()
                        .authoringGroupsAdd("authoringGroup")
                        .authorsAdd("author")
                        .title("title")
                        .citationCrossReferencesAdd(doiXref)
                        .publicationDate("2021")
                        .build();

        LiteratureStatistics statistics =
                new LiteratureStatisticsBuilder().unreviewedProteinCount(10).build();
        LiteratureEntry entry =
                new LiteratureEntryBuilder().citation(citation).statistics(statistics).build();
        LiteratureDocument result = converter.convert(entry);
        assertNotNull(result);

        assertEquals("CI-SUB-5P836CF6C0000", result.getId());
        assertEquals("CI-SUB-5P836CF6C0000", result.getDocumentId());
        assertEquals("doiId", result.getDoi());

        assertEquals("title", result.getTitle());
        assertTrue(result.getAuthor().contains("author"));
        assertNull(result.getJournal());
        assertEquals("2021", result.getPublished());
        assertNull(result.getLitAbstract());
        assertTrue(result.getAuthorGroups().contains("authoringGroup"));
        assertEquals(2, result.getCitationsWith().size());
        assertTrue(result.getCitationsWith().contains("1_uniprotkb"));
        assertTrue(result.getCitationsWith().contains("3_unreviewed"));
    }
}
