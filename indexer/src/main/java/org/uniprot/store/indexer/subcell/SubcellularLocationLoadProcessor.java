package org.uniprot.store.indexer.subcell;

import java.util.Map;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
import org.uniprot.core.impl.StatisticsBuilder;
import org.uniprot.core.json.parser.subcell.SubcellularLocationJsonConfig;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
@Slf4j
public class SubcellularLocationLoadProcessor
        implements ItemProcessor<SubcellularLocationEntry, SubcellularLocationDocument> {

    private final ObjectMapper subcellularLocationObjectMapper;
    // cache to be loaded from context of previous step, see method getStepExecution below
    private Map<String, SubcellularLocationStatisticsReader.SubcellularLocationCount>
            subcellProteinCountMap;

    public SubcellularLocationLoadProcessor() {
        this.subcellularLocationObjectMapper =
                SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public SubcellularLocationDocument process(SubcellularLocationEntry entry) throws Exception {
        SubcellularLocationEntry subcellularLocationEntry = entry;
        if (subcellProteinCountMap.containsKey(entry.getName())) {
            SubcellularLocationStatisticsReader.SubcellularLocationCount count =
                    subcellProteinCountMap.get(entry.getName());
            Statistics statistics =
                    new StatisticsBuilder()
                            .reviewedProteinCount(count.getReviewedProteinCount())
                            .unreviewedProteinCount(count.getUnreviewedProteinCount())
                            .build();
            subcellularLocationEntry =
                    SubcellularLocationEntryBuilder.from(entry).statistics(statistics).build();
        }
        return createSubcellularLocationDocument(subcellularLocationEntry);
    }

    private SubcellularLocationDocument createSubcellularLocationDocument(
            SubcellularLocationEntry entry) {
        byte[] subcellularLocationByte = getSubcellularLocationObjectBinary(entry);

        return SubcellularLocationDocument.builder()
                .id(entry.getId())
                .name(entry.getName())
                .category(entry.getCategory().getName())
                .definition(entry.getDefinition())
                .synonyms(entry.getSynonyms())
                .subcellularlocationObj(subcellularLocationByte)
                .build();
    }

    private byte[] getSubcellularLocationObjectBinary(
            SubcellularLocationEntry subcellularLocation) {
        try {
            return this.subcellularLocationObjectMapper.writeValueAsBytes(subcellularLocation);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse SubcellularLocation to binary json: ", e);
        }
    }

    @BeforeStep
    public void getCrossRefProteinCountMap(
            final StepExecution stepExecution) { // get the cached data from previous step
        log.info(
                "Loading StepExecution Statistics Map SubcellularLocationLoadProcessor.subcellProteinCountMap");
        this.subcellProteinCountMap =
                (Map<String, SubcellularLocationStatisticsReader.SubcellularLocationCount>)
                        stepExecution
                                .getJobExecution()
                                .getExecutionContext()
                                .get(Constants.SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY);
        if (this.subcellProteinCountMap == null) {
            log.error("StepExecution Statistics Map is null");
        }
    }
}
