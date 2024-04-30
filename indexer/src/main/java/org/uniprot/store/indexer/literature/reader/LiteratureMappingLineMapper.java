package org.uniprot.store.indexer.literature.reader;

import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.impl.LiteratureBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 */
@Slf4j
public class LiteratureMappingLineMapper extends DefaultLineMapper<LiteratureEntry> {

    public LiteratureEntry mapLine(String entryString, int lineNumber) throws Exception {
        String[] lineFields = entryString.split("\t");
        if (lineFields.length == 4 || lineFields.length == 5) {
            LiteratureStatistics statistics;
            if (isCommunityEntry(entryString)) {
                statistics =
                        new LiteratureStatisticsBuilder().communityMappedProteinCount(1).build();
            } else {
                statistics =
                        new LiteratureStatisticsBuilder()
                                .computationallyMappedProteinCount(1)
                                .build();
            }

            CrossReference<CitationDatabase> xref =
                    new CrossReferenceBuilder<CitationDatabase>()
                            .database(CitationDatabase.PUBMED)
                            .id(lineFields[2])
                            .build();

            Literature literature =
                    new LiteratureBuilder().citationCrossReferencesAdd(xref).build();

            LiteratureEntry entry =
                    new LiteratureEntryBuilder()
                            .citation(literature)
                            .statistics(statistics)
                            .build();

            return entry;
        } else {
            log.warn(
                    "Unable to parse correctly line number ["
                            + lineNumber
                            + "] with value: "
                            + entryString);
            return null;
        }
    }

    private boolean isCommunityEntry(String line) {
        return line.contains("ORCID");
    }
}
