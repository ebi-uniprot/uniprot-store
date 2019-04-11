package uk.ac.ebi.uniprot.indexer.taxonomy.writers;

import org.apache.solr.common.SolrInputDocument;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;

import uk.ac.ebi.uniprot.search.document.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 *
 * @author lgonzales
 */
public class TaxonomyStrainWriter implements ItemWriter<TaxonomyDocument> {

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;

    public TaxonomyStrainWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends TaxonomyDocument> items){
        for (TaxonomyDocument document: items) {
            SolrInputDocument  solrInputDocument = new SolrInputDocument();
            solrInputDocument.addField("id",document.getId()); //TODO: use search enum that will be created

            Map<String,Object> fieldModifier = new HashMap<>(1);
            fieldModifier.put("add",document.getStrain().get(0));
            solrInputDocument.addField("strain",fieldModifier); //TODO: use search enum that will be created

            this.solrTemplate.saveBean(collection.name(), solrInputDocument);
        }
        this.solrTemplate.softCommit(collection.name());
    }
}