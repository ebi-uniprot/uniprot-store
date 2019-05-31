package uk.ac.ebi.uniprot.indexer.taxonomy.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyStatistics;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyEntryBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyStatisticsBuilder;
import uk.ac.ebi.uniprot.indexer.taxonomy.readers.TaxonomyStatisticsReader;
import uk.ac.ebi.uniprot.json.parser.taxonomy.TaxonomyJsonConfig;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.nio.ByteBuffer;

/**
 *
 * @author lgonzales
 */
public class TaxonomyStatisticsProcessor implements ItemProcessor<TaxonomyStatisticsReader.TaxonomyCount, TaxonomyDocument> {

    private final ObjectMapper jsonMapper;

    public TaxonomyStatisticsProcessor(){
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument process(TaxonomyStatisticsReader.TaxonomyCount taxonomyCount) throws Exception {
        TaxonomyStatistics statistics = new TaxonomyStatisticsBuilder()
                .reviewedProteinCount(taxonomyCount.getReviewedProteinCount())
                .unreviewedProteinCount(taxonomyCount.getUnreviewedProteinCount())
                .referenceProteomeCount(taxonomyCount.getReferenceProteomeCount())
                .completeProteomeCount(taxonomyCount.getCompleteProteomeCount())
                .build();

        TaxonomyEntry entry = new TaxonomyEntryBuilder()
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
