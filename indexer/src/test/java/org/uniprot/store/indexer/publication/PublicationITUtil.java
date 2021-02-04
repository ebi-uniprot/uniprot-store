package org.uniprot.store.indexer.publication;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.JournalArticleBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.search.document.publication.PublicationDocument;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 14/01/2021
 */
public class PublicationITUtil {

    private static final ObjectMapper OBJECT_MAPPER =
            MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();

    public static MappedPublications extractObject(PublicationDocument document)
            throws IOException {
        return OBJECT_MAPPER.readValue(
                document.getPublicationMappedReferences(), MappedPublications.class);
    }

    public static LiteratureDocument createLargeScaleLiterature(int pubmedId) throws Exception {

        CrossReference<CitationDatabase> xref =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.PUBMED)
                        .id(String.valueOf(pubmedId))
                        .build();
        Citation citation = new JournalArticleBuilder().citationCrossReferencesAdd(xref).build();

        LiteratureStatistics statistics =
                new LiteratureStatisticsBuilder()
                        .communityMappedProteinCount(10)
                        .computationallyMappedProteinCount(20)
                        .reviewedProteinCount(30)
                        .unreviewedProteinCount(40)
                        .build();
        LiteratureEntry entry =
                new LiteratureEntryBuilder().citation(citation).statistics(statistics).build();

        byte[] litBytes =
                LiteratureJsonConfig.getInstance()
                        .getFullObjectMapper()
                        .writer()
                        .writeValueAsBytes(entry);

        return LiteratureDocument.builder()
                .id(String.valueOf(pubmedId))
                .isCommunityMapped(true)
                .isComputationallyMapped(true)
                .isUniprotkbMapped(true)
                .literatureObj(ByteBuffer.wrap(litBytes))
                .build();
    }

    public static LiteratureDocument createLargeScaleLiteratureWithOneCount(
            int pubmedId, MappedReferenceType type) throws Exception {

        CrossReference<CitationDatabase> xref =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.PUBMED)
                        .id(String.valueOf(pubmedId))
                        .build();
        Citation citation = new JournalArticleBuilder().citationCrossReferencesAdd(xref).build();

        LiteratureStatisticsBuilder statisticsBuilder = new LiteratureStatisticsBuilder();
        switch (type) {
            case COMMUNITY:
                statisticsBuilder.communityMappedProteinCount(60);
                break;
            case COMPUTATIONAL:
                statisticsBuilder.computationallyMappedProteinCount(70);
                break;
            case UNIPROTKB_REVIEWED:
                statisticsBuilder.reviewedProteinCount(80);
                break;
            case UNIPROTKB_UNREVIEWED:
                statisticsBuilder.unreviewedProteinCount(90);
                break;
        }
        LiteratureStatistics statistics = statisticsBuilder.build();
        LiteratureEntry entry =
                new LiteratureEntryBuilder().citation(citation).statistics(statistics).build();

        byte[] litBytes =
                LiteratureJsonConfig.getInstance()
                        .getFullObjectMapper()
                        .writer()
                        .writeValueAsBytes(entry);

        return LiteratureDocument.builder()
                .id(String.valueOf(pubmedId))
                .isCommunityMapped(true)
                .isComputationallyMapped(true)
                .isUniprotkbMapped(true)
                .literatureObj(ByteBuffer.wrap(litBytes))
                .build();
    }
}
