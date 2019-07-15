package uk.ac.ebi.uniprot.indexer.subcell;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.Criteria;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SimpleQuery;
import uk.ac.ebi.uniprot.cv.subcell.SubcellularLocationEntry;
import uk.ac.ebi.uniprot.cv.subcell.impl.SubcellularLocationEntryImpl;
import uk.ac.ebi.uniprot.json.parser.subcell.SubcellularLocationJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.subcell.SubcellularLocationDocument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
public class SubcellularLocationLoadProcessor implements ItemProcessor<SubcellularLocationEntry, SubcellularLocationDocument> {

    private final ObjectMapper subcellularLocationObjectMapper;
    private final SolrTemplate solrTemplate;

    public SubcellularLocationLoadProcessor(SolrTemplate solrTemplate) {
        this.solrTemplate = solrTemplate;
        this.subcellularLocationObjectMapper = SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public SubcellularLocationDocument process(SubcellularLocationEntry entry) throws Exception {
        SubcellularLocationEntryImpl subcellularLocationEntry = (SubcellularLocationEntryImpl) entry;

        Query query = new SimpleQuery().addCriteria(Criteria.where("id").is(subcellularLocationEntry.getAccession()));
        Optional<SubcellularLocationDocument> optionalDocument = solrTemplate.queryForObject(SolrCollection.subcellularlocation.name(), query, SubcellularLocationDocument.class);
        if (optionalDocument.isPresent()) {
            SubcellularLocationDocument document = optionalDocument.get();

            byte[] subcellularLocationObj = document.getSubcellularlocationObj().array();
            SubcellularLocationEntry statisticsEntry = subcellularLocationObjectMapper.readValue(subcellularLocationObj, SubcellularLocationEntryImpl.class);
            subcellularLocationEntry.setStatistics(statisticsEntry.getStatistics());

            solrTemplate.delete(SolrCollection.subcellularlocation.name(), query);
            solrTemplate.softCommit(SolrCollection.subcellularlocation.name());
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
}
