package uk.ac.ebi.uniprot.indexer.taxonomy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.domain.taxonomy.TaxonomyEntry;
import uk.ac.ebi.uniprot.json.parser.taxonomy.TaxonomyJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author lgonzales
 */
public class TaxonomyEntryWriter implements ItemWriter<TaxonomyEntry> {

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private final ObjectMapper jsonMapper;

    public TaxonomyEntryWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
        jsonMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public void write(List<? extends TaxonomyEntry> items){
        for (TaxonomyEntry entry: items) {
            ByteBuffer taxonomyObj = getTaxonomyBinary(entry);
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            Map<String,Object> fieldModifier = new HashMap<>(1);
            fieldModifier.put("set",taxonomyObj);
            solrInputDocument.addField("taxonomy_obj",fieldModifier);

            solrInputDocument.addField("id",entry.getTaxonId()); //TODO: use search enum that will be created
            this.solrTemplate.saveBean(collection.name(), solrInputDocument);
        }
        this.solrTemplate.softCommit(collection.name());
    }

    private ByteBuffer getTaxonomyBinary(TaxonomyEntry entry) {
        try {
            return ByteBuffer.wrap(jsonMapper.writeValueAsBytes(entry));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse TaxonomyEntry to binary json: ", e);
        }
    }
}
