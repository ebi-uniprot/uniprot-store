package org.uniprot.store.indexer.literature.processor;

import java.nio.ByteBuffer;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.impl.LiteratureBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.store.indexer.literature.reader.LiteratureStatisticsReader;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
@Slf4j
public class LiteratureStatisticsProcessor
        implements ItemProcessor<LiteratureStatisticsReader.LiteratureCount, LiteratureDocument> {

    private final ObjectMapper literatureObjectMapper;

    public LiteratureStatisticsProcessor() {
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureStatisticsReader.LiteratureCount literatureCount)
            throws Exception {
        LiteratureStatistics statistics =
                new LiteratureStatisticsBuilder()
                        .reviewedProteinCount(literatureCount.getReviewedProteinCount())
                        .unreviewedProteinCount(literatureCount.getUnreviewedProteinCount())
                        .build();
        CrossReference<CitationDatabase> pubmedXref =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.PUBMED)
                        .id(String.valueOf(literatureCount.getPubmedId()))
                        .build();
        Literature literature =
                new LiteratureBuilder().citationCrossReferencesAdd(pubmedXref).build();
        LiteratureEntry literatureEntry =
                new LiteratureEntryBuilder().citation(literature).statistics(statistics).build();

        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        builder.id(String.valueOf(literatureCount.getPubmedId()));

        byte[] literatureByte = getLiteratureObjectBinary(literatureEntry);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));

        log.debug("LiteratureStatisticsProcessor entry: " + literatureCount.getPubmedId());
        return builder.build();
    }

    private byte[] getLiteratureObjectBinary(LiteratureEntry literature) {
        try {
            return this.literatureObjectMapper.writeValueAsBytes(literature);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse Literature to binary json: ", e);
        }
    }
}
