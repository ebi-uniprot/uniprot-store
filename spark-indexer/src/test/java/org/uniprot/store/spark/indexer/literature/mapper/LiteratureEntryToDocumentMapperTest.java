package org.uniprot.store.spark.indexer.literature.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.LiteratureBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * @author lgonzales
 * @since 30/03/2021
 */
class LiteratureEntryToDocumentMapperTest {

    @Test
    void canMapLiteratureEntryToDocument() throws Exception {
        LiteratureEntryToDocumentMapper mapper = new LiteratureEntryToDocumentMapper();

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
                        .unreviewedProteinCount(10)
                        .communityMappedProteinCount(10)
                        .computationallyMappedProteinCount(10)
                        .build();
        LiteratureEntry entry =
                new LiteratureEntryBuilder().citation(citation).statistics(statistics).build();
        LiteratureDocument result = mapper.call(entry);
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
        assertTrue(result.getCitationsWith().contains("1_uniprotkb"));
        assertTrue(result.getCitationsWith().contains("2_reviewed"));
        assertTrue(result.getCitationsWith().contains("3_unreviewed"));
        assertTrue(result.getCitationsWith().contains("4_computationally"));
        assertTrue(result.getCitationsWith().contains("5_community"));
    }
}
