package uk.ac.ebi.uniprot.indexer.subcell;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.cv.subcell.SubcellularLocationEntry;
import uk.ac.ebi.uniprot.cv.subcell.SubcellularLocationStatistics;
import uk.ac.ebi.uniprot.cv.subcell.impl.SubcellularLocationEntryImpl;
import uk.ac.ebi.uniprot.cv.subcell.impl.SubcellularLocationStatisticsImpl;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.json.parser.subcell.SubcellularLocationJsonConfig;
import uk.ac.ebi.uniprot.search.document.subcell.SubcellularLocationDocument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
@Slf4j
public class SubcellularLocationLoadProcessor implements ItemProcessor<SubcellularLocationEntry, SubcellularLocationDocument> {

    private final ObjectMapper subcellularLocationObjectMapper;
    // cache to be loaded from context of previous step, see method getStepExecution below
    private Map<String, SubcellularLocationStatisticsReader.SubcellularLocationCount> subcellProteinCountMap;

    public SubcellularLocationLoadProcessor() {
        this.subcellularLocationObjectMapper = SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public SubcellularLocationDocument process(SubcellularLocationEntry entry) throws Exception {
        SubcellularLocationEntryImpl subcellularLocationEntry = (SubcellularLocationEntryImpl) entry;
        if (subcellProteinCountMap.containsKey(entry.getId())) {
            SubcellularLocationStatisticsReader.SubcellularLocationCount count = subcellProteinCountMap.get(entry.getId());
            SubcellularLocationStatistics statistics = new SubcellularLocationStatisticsImpl(count.getReviewedProteinCount(), count.getUnreviewedProteinCount());
            subcellularLocationEntry.setStatistics(statistics);
        }
        return createSubcellularLocationDocument(subcellularLocationEntry);
    }

    private SubcellularLocationDocument createSubcellularLocationDocument(SubcellularLocationEntry entry) {
        byte[] subcellularLocationByte = getSubcellularLocationObjectBinary(entry);

        return SubcellularLocationDocument.builder()
                .id(entry.getAccession())
                .name(entry.getId())
                .category(entry.getCategory().getCategory())
                .content(getContent(entry))
                .subcellularlocationObj(ByteBuffer.wrap(subcellularLocationByte))
                .build();
    }

    private List<String> getContent(SubcellularLocationEntry entry) {
        List<String> content = new ArrayList<>();
        content.add(entry.getAccession());
        content.add(entry.getId());
        content.add(entry.getDefinition());
        content.add(entry.getCategory().toDisplayName());
        if (entry.getSynonyms() != null) {
            content.addAll(entry.getSynonyms());
        }
        return content;
    }

    private byte[] getSubcellularLocationObjectBinary(SubcellularLocationEntry subcellularLocation) {
        try {
            return this.subcellularLocationObjectMapper.writeValueAsBytes(subcellularLocation);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse SubcellularLocation to binary json: ", e);
        }
    }

    @BeforeStep
    public void getCrossRefProteinCountMap(final StepExecution stepExecution) {// get the cached data from previous step
        log.info("Loading StepExecution Statistics Map SubcellularLocationLoadProcessor.subcellProteinCountMap");
        this.subcellProteinCountMap = (Map<String, SubcellularLocationStatisticsReader.SubcellularLocationCount>) stepExecution.getJobExecution()
                .getExecutionContext().get(Constants.SUBCELLULAR_LOCATION_LOAD_STATISTICS_KEY);
        if (this.subcellProteinCountMap == null) {
            log.error("StepExecution Statistics Map is null");
        }
    }

}
