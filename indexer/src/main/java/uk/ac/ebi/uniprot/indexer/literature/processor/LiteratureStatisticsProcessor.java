package uk.ac.ebi.uniprot.indexer.literature.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.domain.literature.LiteratureEntry;
import uk.ac.ebi.uniprot.domain.literature.LiteratureStatistics;
import uk.ac.ebi.uniprot.domain.literature.builder.LiteratureEntryBuilder;
import uk.ac.ebi.uniprot.domain.literature.builder.LiteratureStatisticsBuilder;
import uk.ac.ebi.uniprot.indexer.literature.reader.LiteratureStatisticsReader;
import uk.ac.ebi.uniprot.json.parser.literature.LiteratureJsonConfig;
import uk.ac.ebi.uniprot.search.document.literature.LiteratureDocument;

import java.nio.ByteBuffer;

/**
 * @author lgonzales
 */
@Slf4j
public class LiteratureStatisticsProcessor implements ItemProcessor<LiteratureStatisticsReader.LiteratureCount, LiteratureDocument> {

    private final ObjectMapper literatureObjectMapper;

    public LiteratureStatisticsProcessor() {
        this.literatureObjectMapper = LiteratureJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public LiteratureDocument process(LiteratureStatisticsReader.LiteratureCount literatureCount) throws Exception {
        LiteratureStatistics statistics = new LiteratureStatisticsBuilder()
                .reviewedProteinCount(literatureCount.getReviewedProteinCount())
                .unreviewedProteinCount(literatureCount.getUnreviewedProteinCount())
                .build();
        LiteratureEntry literatureEntry = new LiteratureEntryBuilder()
                .pubmedId(literatureCount.getPubmedId())
                .statistics(statistics)
                .build();

        LiteratureDocument.LiteratureDocumentBuilder builder = LiteratureDocument.builder();
        builder.id(literatureCount.getPubmedId());

        byte[] literatureByte = getLiteratureObjectBinary(literatureEntry);
        builder.literatureObj(ByteBuffer.wrap(literatureByte));

        log.debug("LiteratureStatisticsProcessor entry: " + literatureEntry);
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
