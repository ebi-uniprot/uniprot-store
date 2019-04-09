package uk.ac.ebi.uniprot.indexer.taxonomy.writers;

import org.apache.solr.common.SolrInputDocument;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.configure.SolrCollection;
import uk.ac.ebi.uniprot.indexer.configure.taxonomy.TaxonomyDocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author lgonzales
 */
public class TaxonomyCountWriter  implements ItemWriter<TaxonomyDocument> {

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;

    public TaxonomyCountWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends TaxonomyDocument> items){
        for (TaxonomyDocument document: items) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            if(document.getSwissprotCount() != null){
                Map<String,Object> fieldModifier = new HashMap<>(1);
                fieldModifier.put("set",document.getSwissprotCount());
                solrInputDocument.addField("swissprotCount",fieldModifier);
            }
            if(document.getTremblCount() != null){
                Map<String,Object> fieldModifier = new HashMap<>(1);
                fieldModifier.put("set",document.getTremblCount());
                solrInputDocument.addField("tremblCount",fieldModifier);
            }

            solrInputDocument.addField("id",document.getId()); //TODO: use search enum that will be created
            this.solrTemplate.saveBean(collection.name(), solrInputDocument);
        }
        this.solrTemplate.softCommit(collection.name());
    }
}
