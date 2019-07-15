package uk.ac.ebi.uniprot.indexer.subcell;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.cv.subcell.SubcellularLocationEntry;
import uk.ac.ebi.uniprot.cv.subcell.SubcellularLocationStatistics;
import uk.ac.ebi.uniprot.cv.subcell.impl.SubcellularLocationEntryImpl;
import uk.ac.ebi.uniprot.cv.subcell.impl.SubcellularLocationStatisticsImpl;
import uk.ac.ebi.uniprot.json.parser.subcell.SubcellularLocationJsonConfig;
import uk.ac.ebi.uniprot.search.document.subcell.SubcellularLocationDocument;

import java.nio.ByteBuffer;

/**
 * @author lgonzales
 * @since 2019-07-12
 */
public class SubcellularLocationStatisticsProcessor implements ItemProcessor<SubcellularLocationStatisticsReader.SubcellularLocationCount, SubcellularLocationDocument> {

    private final ObjectMapper subcellularLocationObjectMapper;

    public SubcellularLocationStatisticsProcessor() {
        this.subcellularLocationObjectMapper = SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public SubcellularLocationDocument process(SubcellularLocationStatisticsReader.SubcellularLocationCount subcellularLocationCount) throws Exception {
        SubcellularLocationStatistics statistics = new SubcellularLocationStatisticsImpl(subcellularLocationCount.getReviewedProteinCount(), subcellularLocationCount.getUnreviewedProteinCount());
        SubcellularLocationEntryImpl subcellularLocationEntry = new SubcellularLocationEntryImpl();
        subcellularLocationEntry.setStatistics(statistics);

        SubcellularLocationDocument.SubcellularLocationDocumentBuilder builder = SubcellularLocationDocument.builder();
        builder.id(subcellularLocationCount.getSubcellularLocationId());

        byte[] subcellularLocationByte = getSubcellularLocationObjectBinary(subcellularLocationEntry);
        builder.subcellularlocationObj(ByteBuffer.wrap(subcellularLocationByte));

        return builder.build();
    }

    private byte[] getSubcellularLocationObjectBinary(SubcellularLocationEntry subcellularLocation) {
        try {
            return this.subcellularLocationObjectMapper.writeValueAsBytes(subcellularLocation);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse SubcellularLocation to binary json: ", e);
        }
    }
}
