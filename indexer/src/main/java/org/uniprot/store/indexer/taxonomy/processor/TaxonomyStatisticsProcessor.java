package org.uniprot.store.indexer.taxonomy.processor;

import java.nio.ByteBuffer;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.builder.TaxonomyStatisticsBuilder;
import org.uniprot.store.indexer.taxonomy.readers.TaxonomyStatisticsReader;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** @author lgonzales */
public class TaxonomyStatisticsProcessor
        implements ItemProcessor<TaxonomyStatisticsReader.TaxonomyCount, TaxonomyDocument> {

    private final ObjectMapper jsonMapper;

    public TaxonomyStatisticsProcessor() {
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument process(TaxonomyStatisticsReader.TaxonomyCount taxonomyCount)
            throws Exception {
        TaxonomyStatistics statistics =
                new TaxonomyStatisticsBuilder()
                        .reviewedProteinCount(taxonomyCount.getReviewedProteinCount())
                        .unreviewedProteinCount(taxonomyCount.getUnreviewedProteinCount())
                        .referenceProteomeCount(taxonomyCount.getReferenceProteomeCount())
                        .proteomeCount(taxonomyCount.getProteomeCount())
                        .build();

        TaxonomyEntry entry =
                new TaxonomyEntryBuilder()
                        .taxonId(taxonomyCount.getTaxId())
                        .statistics(statistics)
                        .build();
        return TaxonomyDocument.builder()
                .id(String.valueOf(taxonomyCount.getTaxId()))
                .taxId(taxonomyCount.getTaxId())
                .taxonomyObj(getTaxonomyBinary(entry))
                .build();
    }

    private ByteBuffer getTaxonomyBinary(TaxonomyEntry entry) {
        try {
            return ByteBuffer.wrap(jsonMapper.writeValueAsBytes(entry));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse TaxonomyEntry to binary json: ", e);
        }
    }
}
